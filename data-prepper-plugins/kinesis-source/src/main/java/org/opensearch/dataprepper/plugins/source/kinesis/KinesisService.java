/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.kinesis;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.kinesis.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.kinesis.leader.ShardManager;
import org.opensearch.dataprepper.plugins.source.kinesis.stream.ShardConsumerFactory;
import org.opensearch.dataprepper.plugins.source.kinesis.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.source.kinesis.utils.BackoffCalculator;
import org.opensearch.dataprepper.plugins.source.kinesis.utils.KinesisSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KinesisService {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisService.class);

//    private final List<TableConfig> tableConfigs;

    private final EnhancedSourceCoordinator coordinator;

    private final DynamoDbAsyncClient dynamoDbClient;

    private final KinesisSourceConfig kinesisSourceConfig;
    //
    private final KinesisAsyncClient kinesisClient;

//    private final S3Client s3Client;

    private final ShardManager shardManager;

    private final ExecutorService executor;

    private final PluginMetrics pluginMetrics;

    private final KinesisSourceAggregateMetrics kinesisSourceAggregateMetrics;

    private final AcknowledgementSetManager acknowledgementSetManager;


    public KinesisService(final EnhancedSourceCoordinator coordinator,
                           final KinesisSourceConfig sourceConfig,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.kinesisSourceConfig = sourceConfig;
        this.kinesisSourceAggregateMetrics = new KinesisSourceAggregateMetrics();

        // Initialize AWS clients
        dynamoDbClient = DynamoDbAsyncClient.builder()
                .credentialsProvider(sourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .region(sourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .build();
        this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder()
                        .credentialsProvider(sourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                        .region(sourceConfig.getAwsAuthenticationOptions().getAwsRegion())
        );

        // A shard manager is responsible to retrieve the shard information from streams.
        shardManager = new ShardManager(kinesisClient, kinesisSourceAggregateMetrics);
//        tableConfigs = sourceConfig.getTableConfigs();
        executor = Executors.newFixedThreadPool(2);
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {

        LOG.info("Start running Kinesis service");

        ShardConsumerFactory consumerFactory = new ShardConsumerFactory(coordinator, kinesisClient, pluginMetrics, kinesisSourceAggregateMetrics, buffer, kinesisSourceConfig.getStreams().get(0));
        Runnable streamScheduler = new StreamScheduler(coordinator, consumerFactory, pluginMetrics, acknowledgementSetManager, kinesisSourceConfig, new BackoffCalculator(false));
        // leader scheduler will handle the initialization
        Runnable leaderScheduler = new LeaderScheduler(coordinator, dynamoDbClient, shardManager, kinesisSourceConfig);

        // May consider start or shutdown the scheduler on demand
        // Currently, event after the exports are done, the related scheduler will not be shutdown
        // This is because in the future we may support incremental exports.
        executor.submit(leaderScheduler);
        executor.submit(streamScheduler);
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        LOG.info("shutdown DynamoDB schedulers");
        executor.shutdownNow();
    }

}
