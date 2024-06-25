package org.opensearch.dataprepper.plugins.source.kinesis;

import org.opensearch.dataprepper.metrics.PluginMetrics;
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
import org.opensearch.dataprepper.plugins.source.kinesis.metrics.MicrometerMetricFactory;
import org.opensearch.dataprepper.plugins.source.kinesis.processor.KinesisRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "kinesis", pluginType = Source.class, pluginConfigurationType = KinesisSourceConfig.class)
public class KinesisSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

    private static final int DEFAULT_MAX_RECORDS = 10000;
//    private static final int DEFAULT_AWS_CONNECTION_TIME_OUT_SECONDS = 60;
//    private static final int MAX_CONCURRENCY = 100;

    private static final int IDEL_TIME_BETWEEN_READS_IN_MILLIS = 250;


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

    private Scheduler scheduler;
    private final KinesisAsyncClient kinesisClient;
    private final DynamoDbAsyncClient dynamoClient;
    private final CloudWatchAsyncClient cloudWatchClient;
    private final String workerIdentifier;

    @DataPrepperPluginConstructor
    public KinesisSource(final KinesisSourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final PipelineDescription pipelineDescription) {

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

    }
    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        final ShardRecordProcessorFactory processorFactory = new KinesisRecordProcessorFactory(buffer, codec);

        final List<String> streamNames = sourceConfig.getStreams().stream().map(KinesisStreamConfig::getName).collect(Collectors.toList());
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                new KinesisMultiStreamTracker(kinesisClient, streamNames), applicationName, kinesisClient, dynamoClient, cloudWatchClient, workerIdentifier, processorFactory).tableName(tableName);

        sourceConfig.getStreams().forEach(stream -> {
            if (stream.getConsumerStrategy() == KinesisStreamConfig.ConsumerStrategy.POLLING) {
                configsBuilder.retrievalConfig().retrievalSpecificConfig(
                        new PollingConfig(kinesisClient)
                                .maxRecords(DEFAULT_MAX_RECORDS)
                                .idleTimeBetweenReadsInMillis(IDEL_TIME_BETWEEN_READS_IN_MILLIS));
            }
        });

        scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsFactory(new MicrometerMetricFactory(pluginMetrics)),
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
}
