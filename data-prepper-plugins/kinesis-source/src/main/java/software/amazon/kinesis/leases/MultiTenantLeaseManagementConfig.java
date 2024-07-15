package software.amazon.kinesis.leases;

import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.leases.dynamodb.MultiTenantDynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.MultiTenantDynamoDBLeaseSerializer;

@Accessors(fluent = true)
public class MultiTenantLeaseManagementConfig extends LeaseManagementConfig {

    private HierarchicalShardSyncer hierarchicalShardSyncer;
    private final String applicationName;
    private LeaseSerializer leaseSerializer;

    private LeaseManagementFactory leaseManagementFactory;

    public HierarchicalShardSyncer hierarchicalShardSyncer() {
        if (hierarchicalShardSyncer == null) {
            hierarchicalShardSyncer = new MultiTenantHierarchicalShardSyncer();
        }
        return hierarchicalShardSyncer;
    }

    private LeaseSerializer leaseSerializer() {
        if (leaseSerializer == null) {
            leaseSerializer = new MultiTenantDynamoDBLeaseSerializer(applicationName);
        }
        return leaseSerializer;
    }

    public MultiTenantLeaseManagementConfig(String tableName, String applicationName, DynamoDbAsyncClient dynamoDBClient, KinesisAsyncClient kinesisClient, String workerIdentifier) {
        super(tableName, dynamoDBClient, kinesisClient, workerIdentifier);
        this.applicationName = applicationName;
    }

    public static MultiTenantLeaseManagementConfig build(ConfigsBuilder c) {
        final MultiTenantLeaseManagementConfig leaseManagementConfig = new MultiTenantLeaseManagementConfig(
                c.tableName(),
                c.applicationName(),
                c.dynamoDBClient(),
                c.kinesisClient(),
                c.workerIdentifier());
        return leaseManagementConfig;
    }

    public LeaseManagementFactory leaseManagementFactory() {
        if (leaseManagementFactory == null) {
            Validate.notEmpty(streamName(), "Stream name is empty");
            leaseManagementFactory = new MultiTenantDynamoDBLeaseManagementFactory(kinesisClient(),
                    dynamoDBClient(),
                    tableName(),
                    applicationName,
                    workerIdentifier(),
                    executorService(),
                    failoverTimeMillis(),
                    enablePriorityLeaseAssignment(),
                    epsilonMillis(),
                    maxLeasesForWorker(),
                    maxLeasesToStealAtOneTime(),
                    maxLeaseRenewalThreads(),
                    cleanupLeasesUponShardCompletion(),
                    ignoreUnexpectedChildShards(),
                    shardSyncIntervalMillis(),
                    consistentReads(),
                    listShardsBackoffTimeInMillis(),
                    maxListShardsRetryAttempts(),
                    maxCacheMissesBeforeReload(),
                    listShardsCacheAllowedAgeInSeconds(),
                    cacheMissWarningModulus(),
                    initialLeaseTableReadCapacity(),
                    initialLeaseTableWriteCapacity(),
                    hierarchicalShardSyncer(),
                    tableCreatorCallback(),
                    dynamoDbRequestTimeout(),
                    billingMode(),
                    leaseTableDeletionProtectionEnabled(),
                    tags(),
                    leaseSerializer(),
                    customShardDetectorProvider(),
                    true,
                    leaseCleanupConfig());
        }
        return leaseManagementFactory;
    }

    /**
     * Vends LeaseManagementFactory that performs serde based on leaseSerializer and shard sync based on isMultiStreamingMode
     * @param leaseSerializer
     * @param isMultiStreamingMode
     * @return LeaseManagementFactory
     */
    public LeaseManagementFactory leaseManagementFactory(final LeaseSerializer leaseSerializer, boolean isMultiStreamingMode) {
        if (leaseManagementFactory == null) {
            leaseManagementFactory = new MultiTenantDynamoDBLeaseManagementFactory(kinesisClient(),
                    dynamoDBClient(),
                    tableName(),
                    applicationName,
                    workerIdentifier(),
                    executorService(),
                    failoverTimeMillis(),
                    enablePriorityLeaseAssignment(),
                    epsilonMillis(),
                    maxLeasesForWorker(),
                    maxLeasesToStealAtOneTime(),
                    maxLeaseRenewalThreads(),
                    cleanupLeasesUponShardCompletion(),
                    ignoreUnexpectedChildShards(),
                    shardSyncIntervalMillis(),
                    consistentReads(),
                    listShardsBackoffTimeInMillis(),
                    maxListShardsRetryAttempts(),
                    maxCacheMissesBeforeReload(),
                    listShardsCacheAllowedAgeInSeconds(),
                    cacheMissWarningModulus(),
                    initialLeaseTableReadCapacity(),
                    initialLeaseTableWriteCapacity(),
                    hierarchicalShardSyncer(),
                    tableCreatorCallback(),
                    dynamoDbRequestTimeout(),
                    billingMode(),
                    leaseTableDeletionProtectionEnabled(),
                    tags(),
                    leaseSerializer(),
                    customShardDetectorProvider(),
                    isMultiStreamingMode,
                    leaseCleanupConfig());
        }
        return leaseManagementFactory;
    }
}
