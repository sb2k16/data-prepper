package org.opensearch.dataprepper.plugins.source.kinesis;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.kinesis.configuration.AwsAuthenticationConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

public class ClientFactory {
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsAuthenticationConfig awsAuthenticationConfig;

    public ClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier,
                         final AwsAuthenticationConfig awsAuthenticationConfig) {
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
        this.awsAuthenticationConfig = awsAuthenticationConfig;
    }

    public DynamoDbAsyncClient buildDynamoDBClient() {
        return DynamoDbAsyncClient.builder()
                .credentialsProvider(awsAuthenticationConfig.authenticateAwsConfiguration())
                .region(awsAuthenticationConfig.getAwsRegion())
                .build();
    }

    public KinesisAsyncClient buildKinesisAsyncClient() {
        return KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder()
                    .credentialsProvider(awsAuthenticationConfig.authenticateAwsConfiguration())
                    .region(awsAuthenticationConfig.getAwsRegion())
        );
    }

    public CloudWatchAsyncClient buildCloudWatchAsyncClient() {
        return CloudWatchAsyncClient.builder()
                .credentialsProvider(awsAuthenticationConfig.authenticateAwsConfiguration())
                .region(awsAuthenticationConfig.getAwsRegion())
                .build();
    }
}