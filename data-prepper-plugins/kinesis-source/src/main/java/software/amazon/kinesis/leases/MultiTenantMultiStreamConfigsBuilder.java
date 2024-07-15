package software.amazon.kinesis.leases;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.StreamTracker;

@EqualsAndHashCode(callSuper = true)
public class MultiTenantMultiStreamConfigsBuilder extends ConfigsBuilder {
    public MultiTenantMultiStreamConfigsBuilder(@NonNull String streamName, @NonNull String applicationName, @NonNull KinesisAsyncClient kinesisClient, @NonNull DynamoDbAsyncClient dynamoDBClient, @NonNull CloudWatchAsyncClient cloudWatchClient, @NonNull String workerIdentifier, @NonNull ShardRecordProcessorFactory shardRecordProcessorFactory) {
        super(streamName, applicationName, kinesisClient, dynamoDBClient, cloudWatchClient, workerIdentifier, shardRecordProcessorFactory);
    }

    public MultiTenantMultiStreamConfigsBuilder(@NonNull Arn streamArn, @NonNull String applicationName, @NonNull KinesisAsyncClient kinesisClient, @NonNull DynamoDbAsyncClient dynamoDBClient, @NonNull CloudWatchAsyncClient cloudWatchClient, @NonNull String workerIdentifier, @NonNull ShardRecordProcessorFactory shardRecordProcessorFactory) {
        super(streamArn, applicationName, kinesisClient, dynamoDBClient, cloudWatchClient, workerIdentifier, shardRecordProcessorFactory);
    }

    public MultiTenantMultiStreamConfigsBuilder(@NonNull StreamTracker streamTracker, @NonNull String applicationName, @NonNull KinesisAsyncClient kinesisClient, @NonNull DynamoDbAsyncClient dynamoDBClient, @NonNull CloudWatchAsyncClient cloudWatchClient, @NonNull String workerIdentifier, @NonNull ShardRecordProcessorFactory shardRecordProcessorFactory) {
        super(streamTracker, applicationName, kinesisClient, dynamoDBClient, cloudWatchClient, workerIdentifier, shardRecordProcessorFactory);
    }

    @Override
    public MultiTenantLeaseManagementConfig leaseManagementConfig() {
        final MultiTenantLeaseManagementConfig config = MultiTenantLeaseManagementConfig.build(this);
        return config;
    }
}
