package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.RecordsPublisher;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutRecordsPublisher;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class MultiStreamFanOutRetrievalFactory implements RetrievalFactory {

    private final KinesisAsyncClient kinesisClient;
    private final String defaultStreamName;
    private final String defaultConsumerArn;
    private final Function<StreamIdentifier, String> consumerArnCreator;

    private final Map<StreamIdentifier, String> implicitConsumerArnTracker = new HashMap<>();

    public MultiStreamFanOutRetrievalFactory(KinesisAsyncClient kinesisClient, String defaultStreamName, String defaultConsumerArn, Function<StreamIdentifier, String> consumerArnCreator) {
        this.kinesisClient = kinesisClient;
        this.defaultStreamName = defaultStreamName;
        this.defaultConsumerArn = defaultConsumerArn;
        this.consumerArnCreator = consumerArnCreator;
    }

    @Override
    public RecordsPublisher createGetRecordsCache(
            @NonNull final ShardInfo shardInfo,
            @NonNull final StreamConfig streamConfig,
            @Nullable final MetricsFactory metricsFactory) {
        final Optional<String> streamIdentifierStr = shardInfo.streamIdentifierSerOpt();
        return streamIdentifierStr.map(s -> new FanOutRecordsPublisher(
                kinesisClient,
                shardInfo.shardId(),
                getOrCreateConsumerArn(streamConfig.streamIdentifier(), streamConfig.consumerArn()),
                s)).orElseGet(() -> new FanOutRecordsPublisher(
                kinesisClient,
                shardInfo.shardId(),
                getOrCreateConsumerArn(streamConfig.streamIdentifier(), defaultConsumerArn)));
    }

    private String getOrCreateConsumerArn(StreamIdentifier streamIdentifier, String consumerArn) {
        return consumerArn != null
                ? consumerArn
                : implicitConsumerArnTracker.computeIfAbsent(
                streamIdentifier, sId -> consumerArnCreator.apply(streamIdentifier));
    }
}
