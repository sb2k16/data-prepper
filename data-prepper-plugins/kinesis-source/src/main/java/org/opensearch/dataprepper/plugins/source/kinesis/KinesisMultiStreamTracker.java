package org.opensearch.dataprepper.plugins.source.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private static final String COLON = ":";

    private final KinesisAsyncClient kinesisClient;
    private final List<String> streamNames;

    public KinesisMultiStreamTracker(KinesisAsyncClient kinesisClient, List<String> streamNames) {
        this.kinesisClient = kinesisClient;
        this.streamNames = streamNames;
    }

    @Override
    public List<StreamConfig> streamConfigList()  {
        List<StreamConfig> streamConfigList = new ArrayList<>();
        for (String streamName : streamNames) {
            StreamConfig streamConfig;
            try {
                streamConfig = getStreamConfig(streamName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            streamConfigList.add(streamConfig);
        }
        return streamConfigList;
    }

    private StreamConfig getStreamConfig(String kinesisStreamName) throws Exception {
        StreamIdentifier sourceStreamIdentifier = getStreamIdentifier(kinesisStreamName);
        return new StreamConfig(sourceStreamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));
    }

    private StreamIdentifier getStreamIdentifier(String streamName) throws Exception {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();
        DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
        String streamIdentifierString = getStreamIdentifierString(describeStreamResponse.streamDescription());
        return StreamIdentifier.multiStreamInstance(streamIdentifierString);
    }

    private String getStreamIdentifierString(StreamDescription streamDescription) {
        String accountId = streamDescription.streamARN().split(COLON)[4];
        long creationEpochSecond = streamDescription.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamDescription.streamName(), String.valueOf(creationEpochSecond));
    }

    /**
     * Setting the deletion policy as autodetect and release shard lease with a wait time of 10 sec
     */
    @Override
    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
        return new FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy() {
            @Override
            public Duration waitPeriodToDeleteFormerStreams() {
                return Duration.ofSeconds(10);
            }
        };

    }
}
