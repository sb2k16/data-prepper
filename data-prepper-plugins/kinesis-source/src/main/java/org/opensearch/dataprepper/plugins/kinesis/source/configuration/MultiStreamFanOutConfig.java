package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import org.apache.commons.lang3.ObjectUtils;
import org.opensearch.dataprepper.plugins.kinesis.source.KinesisClientApiHandler;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;

public class MultiStreamFanOutConfig extends FanOutConfig {

    private final KinesisClientApiHandler kinesisClientApiHandler;

    public MultiStreamFanOutConfig(final KinesisAsyncClient kinesisAsyncClient, final KinesisClientApiHandler kinesisClientApiHandler) {
        super(kinesisAsyncClient);
        this.kinesisClientApiHandler = kinesisClientApiHandler;
    }

    @Override
    public RetrievalFactory retrievalFactory() {
        return new MultiStreamFanOutRetrievalFactory(this.kinesisClient(), this.applicationName(), this.consumerArn(), this::getOrCreateConsumerArn);
    }

    private String getOrCreateConsumerArn(StreamIdentifier streamIdentifier) {
        String consumerToCreate = ObjectUtils.firstNonNull(consumerName(), applicationName());
        DataPrepperFanOutConsumerRegistration registration = createConsumerRegistration(kinesisClient(), streamIdentifier, consumerToCreate);
        try {
            return registration.getOrCreateStreamConsumerArn();
        } catch (DependencyException e) {
            throw new RuntimeException(e);
        }
    }

    protected DataPrepperFanOutConsumerRegistration createConsumerRegistration(
            KinesisAsyncClient client, StreamIdentifier streamIdentifier, String consumerToCreate) {

        final String streamIdentifierString = kinesisClientApiHandler.getStreamIdentifierString(
                streamIdentifier.accountIdOptional().orElseThrow(),
                streamIdentifier.streamName(),
                streamIdentifier.streamCreationEpochOptional().orElseThrow()
        );
        final String streamArn = kinesisClientApiHandler.getStreamArnFromStreamIdentifier(streamIdentifierString);

        DataPrepperFanOutConsumerRegistration dataPrepperFanOutConsumerRegistration = new DataPrepperFanOutConsumerRegistration(
                client,
                streamIdentifier.streamName(),
                consumerToCreate,
                maxDescribeStreamSummaryRetries(),
                maxDescribeStreamConsumerRetries(),
                registerStreamConsumerRetries(),
                retryBackoffMillis(),
                streamArn
        );
        return dataPrepperFanOutConsumerRegistration;
    }
}