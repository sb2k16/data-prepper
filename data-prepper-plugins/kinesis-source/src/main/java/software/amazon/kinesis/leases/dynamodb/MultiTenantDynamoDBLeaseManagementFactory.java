package software.amazon.kinesis.leases.dynamodb;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.LeaseCleanupConfig;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.awssdk.services.dynamodb.model.Tag;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;


public class MultiTenantDynamoDBLeaseManagementFactory extends DynamoDBLeaseManagementFactory {
    private final Collection<Tag> tags;
    private final String tableName;
    private final String applicationName;
    private LeaseSerializer leaseSerializer;

    private LeaseSerializer leaseSerializer() {
        if (leaseSerializer == null) {
            leaseSerializer = new MultiTenantDynamoDBLeaseSerializer(applicationName);
        }
        return leaseSerializer;
    }

    public MultiTenantDynamoDBLeaseManagementFactory(final KinesisAsyncClient kinesisClient,
                                                                final DynamoDbAsyncClient dynamoDBClient, final String tableName, final String applicationName,
                                                                final String workerIdentifier,
                                                                final ExecutorService executorService, final long failoverTimeMillis,
                                                                final boolean enablePriorityLeaseAssignment, final long epsilonMillis,
                                                                final int maxLeasesForWorker, final int maxLeasesToStealAtOneTime, final int maxLeaseRenewalThreads,
                                                                final boolean cleanupLeasesUponShardCompletion, final boolean ignoreUnexpectedChildShards,
                                                                final long shardSyncIntervalMillis, final boolean consistentReads, final long listShardsBackoffTimeMillis,
                                                                final int maxListShardsRetryAttempts, final int maxCacheMissesBeforeReload,
                                                                final long listShardsCacheAllowedAgeInSeconds, final int cacheMissWarningModulus,
                                                                final long initialLeaseTableReadCapacity, final long initialLeaseTableWriteCapacity,
                                                                final HierarchicalShardSyncer hierarchicalShardSyncer, final TableCreatorCallback tableCreatorCallback,
                                                                Duration dynamoDbRequestTimeout, BillingMode billingMode, final boolean leaseTableDeletionProtectionEnabled,
                                                                Collection<Tag> tags, LeaseSerializer leaseSerializer,
                                                                Function<StreamConfig, ShardDetector> customShardDetectorProvider, boolean isMultiStreamMode,
                                                                LeaseCleanupConfig leaseCleanupConfig) {
        super(
                kinesisClient,
                dynamoDBClient,
                tableName,
                workerIdentifier,
                executorService,
                failoverTimeMillis,
                enablePriorityLeaseAssignment,
                epsilonMillis,
                maxLeasesForWorker,
                maxLeasesToStealAtOneTime,
                maxLeaseRenewalThreads,
                cleanupLeasesUponShardCompletion,
                ignoreUnexpectedChildShards,
                shardSyncIntervalMillis,
                consistentReads,
                listShardsBackoffTimeMillis,
                maxListShardsRetryAttempts,
                maxCacheMissesBeforeReload,
                listShardsCacheAllowedAgeInSeconds,
                cacheMissWarningModulus,
                initialLeaseTableReadCapacity,
                initialLeaseTableWriteCapacity,
                hierarchicalShardSyncer,
                tableCreatorCallback,
                dynamoDbRequestTimeout,
                billingMode,
                leaseTableDeletionProtectionEnabled,
                tags,
                leaseSerializer,
                customShardDetectorProvider,
                isMultiStreamMode,
                leaseCleanupConfig);

        this.tags = tags;
        this.tableName = tableName;
        this.applicationName = applicationName;
    }

    @Override
    public DynamoDBLeaseRefresher createLeaseRefresher() {
        return new MultiTenantDynamoDBLeaseRefresher(applicationName, getTableName(), getDynamoDBClient(), leaseSerializer(),
                isConsistentReads(), getTableCreatorCallback(), getDynamoDbRequestTimeout(), getBillingMode(), false, getTags());
    }

    @Override
    public LeaseCoordinator createLeaseCoordinator(final MetricsFactory metricsFactory) {
        return new DynamoDBLeaseCoordinator(this.createLeaseRefresher(),
                this.getWorkerIdentifier(),
                this.getFailoverTimeMillis(),
                this.getEpsilonMillis(),
                this.getMaxLeasesForWorker(),
                this.getMaxLeasesToStealAtOneTime(),
                this.getMaxLeaseRenewalThreads(),
                this.getInitialLeaseTableReadCapacity(),
                this.getInitialLeaseTableWriteCapacity(),
                metricsFactory);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

}