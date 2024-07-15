package org.opensearch.dataprepper.plugins.source.kinesis;

import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.source.kinesis.codec.DefaultCodec;
import org.opensearch.dataprepper.plugins.source.kinesis.processor.KinesisShardRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.MultiTenantMultiStreamConfigsBuilder;
import software.amazon.kinesis.leases.MultiTenantLeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.MultiTenantDynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.MultiTenantDynamoDBLeaseSerializer;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "kinesis", pluginType = Source.class, pluginConfigurationType = KinesisSourceConfig.class)
public class KinesisSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

    private static final int DEFAULT_MAX_RECORDS = 10000;
    private static final int IDLE_TIME_BETWEEN_READS_IN_MILLIS = 250;


    public static final long DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 2 * 60 * 1000L;

    // default to false
    private static final boolean SKIP_SHARD_SYNC_AT_START_UP_IF_LEASES_EXIST = false;

    // 60s
    private static final int SHARD_SYNC_INTERVAL_MILLIS = 60000;

    // 10s
    private static final int FAILOVER_TIME_MILLIS = 10000;


    private final PluginMetrics pluginMetrics;
    private final KinesisSourceConfig sourceConfig;
    private final InputCodec codec;

    private final String applicationName;
    private final String tableName;
    private final String pipelineName;
    private final AcknowledgementSetManager acknowledgementSetManager;

    private Scheduler scheduler;
    private final KinesisAsyncClient kinesisClient;
    private final DynamoDbAsyncClient dynamoClient;
    private final CloudWatchAsyncClient cloudWatchClient;
    private final String workerIdentifier;
    private final ExecutorService executorService;

    @DataPrepperPluginConstructor
    public KinesisSource(final KinesisSourceConfig sourceConfig,
                         final PluginMetrics pluginMetrics,
                         final PluginFactory pluginFactory,
                         final PipelineDescription pipelineDescription,
                         final AcknowledgementSetManager acknowledgementSetManager) {
        this.acknowledgementSetManager = acknowledgementSetManager;

        LOG.info("Creating a Kinesis Source");
        this.sourceConfig = sourceConfig;
//        this.pluginFactory = pluginFactory;
        this.pluginMetrics = pluginMetrics;

        // Get codec
        final PluginModel codecConfiguration = sourceConfig.getCodec();
        if (codecConfiguration == null) {
            this.codec = new DefaultCodec();
        } else {
            final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
            this.codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
        }

        pipelineName = pipelineDescription.getPipelineName();
        // Application Name is used in many places
        // Such as DynamoDB table name and CloudWatch Metrics name.
        // The name of an Amazon Kinesis Data Streams application identifies the application. Each of your applications must have a unique name that is scoped to the AWS account and Region used by the application. This name is used as a name for the control table in Amazon DynamoDB and the namespace for Amazon CloudWatch metrics.
        applicationName = pipelineName ; //+ "-" + streamName;
        tableName = applicationName;

        workerIdentifier = new WorkerIdentifierGenerator().generate();

        // Initialize AWS clients
        AwsAuthenticationOptions awsAuthenticationOptions = sourceConfig.getAwsAuthenticationOptions();

        //The default HTTP client for asynchronous operations in the AWS SDK for Java 2.x is the Netty-based NettyNioAsyncHttpClient.
//        SdkAsyncHttpClient asyncHttpClient = NettyNioAsyncHttpClient.builder()
//                .connectionTimeout(Duration.ofSeconds(DEFAULT_AWS_CONNECTION_TIME_OUT_SECONDS))
//                .maxConcurrency(MAX_CONCURRENCY)
//                .build();


        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder()
//                        .httpClient(asyncHttpClient)
                        .credentialsProvider(awsAuthenticationOptions.authenticateAwsConfiguration())
                        .region(awsAuthenticationOptions.getAwsRegion())
        );

        this.dynamoClient = DynamoDbAsyncClient.builder()
//                .httpClient(asyncHttpClient)
                .credentialsProvider(awsAuthenticationOptions.authenticateAwsConfiguration())
                .region(awsAuthenticationOptions.getAwsRegion())
                .build();

        this.cloudWatchClient = CloudWatchAsyncClient.builder()
//                .httpClient(asyncHttpClient)
                .credentialsProvider(awsAuthenticationOptions.authenticateAwsConfiguration())
                .region(awsAuthenticationOptions.getAwsRegion())
                .build();

        executorService = Executors.newFixedThreadPool(4, BackgroundThreadFactory.defaultExecutorThreadFactory("s3-source-sqs"));
    }
    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        final ShardRecordProcessorFactory processorFactory = new KinesisShardRecordProcessorFactory(
                buffer, codec, sourceConfig, acknowledgementSetManager, pluginMetrics);

        MultiTenantMultiStreamConfigsBuilder configsBuilder = new MultiTenantMultiStreamConfigsBuilder(
                new KinesisMultiStreamTracker(kinesisClient, sourceConfig, applicationName),
                applicationName, kinesisClient, dynamoClient, cloudWatchClient, workerIdentifier, processorFactory);
        configsBuilder.tableName("KinesisDynamoDBLeaseCoordinationTable");

        sourceConfig.getStreams().forEach(stream -> {
            if (stream.getConsumerStrategy() == KinesisStreamConfig.ConsumerStrategy.POLLING) {
                configsBuilder.retrievalConfig().retrievalSpecificConfig(
                        new PollingConfig(kinesisClient)
                                .maxRecords(DEFAULT_MAX_RECORDS)
                                .idleTimeBetweenReadsInMillis(IDLE_TIME_BETWEEN_READS_IN_MILLIS));
            }
        });

        MultiTenantDynamoDBLeaseManagementFactory multiTenantDynamoDBLeaseManagementFactory =
                (MultiTenantDynamoDBLeaseManagementFactory) configsBuilder
                        .leaseManagementConfig()
                        .leaseManagementFactory(new MultiTenantDynamoDBLeaseSerializer(applicationName), true);

        scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig()
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .leaseManagementFactory(multiTenantDynamoDBLeaseManagementFactory),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    @Override
    public void stop() {
        LOG.info("Stop request received for Kinesis Source");

        Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
        LOG.info("Waiting up to 20 seconds for shutdown to complete.");
        try {
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.info("Interrupted while waiting for graceful shutdown. Continuing.");
        } catch (ExecutionException e) {
            LOG.error("Exception while executing graceful shutdown.", e);
        } catch (TimeoutException e) {
            LOG.error("Timeout while waiting for shutdown. Scheduler may not have exited.");
        }
        LOG.info("Completed, shutting down now.");
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return sourceConfig.isAcknowledgments();
    }
}
