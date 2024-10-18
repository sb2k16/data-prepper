/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import com.amazonaws.arn.Arn;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamSummary;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private static final String COLON = ":";

    private final KinesisAsyncClient kinesisClient;
    private final KinesisSourceConfig sourceConfig;
    private final String applicationName;

    public KinesisMultiStreamTracker(KinesisAsyncClient kinesisClient, final KinesisSourceConfig sourceConfig, final String applicationName) {
        this.kinesisClient = kinesisClient;
        this.sourceConfig = sourceConfig;
        this.applicationName = applicationName;
    }

    @Override
    public List<StreamConfig> streamConfigList()  {
        List<StreamIdentifier> streamIdentifiers = getAllStreamIdentifiers();
        List<StreamConfig> streamConfigList = new ArrayList<>();
        for (KinesisStreamConfig kinesisStreamConfig : sourceConfig.getStreams()) {
            Optional<StreamIdentifier> streamIdentifier = getSpecificStreamIdentifier(kinesisStreamConfig.getName(), streamIdentifiers);
            if (streamIdentifier.isPresent()) {
                streamConfigList.add(new StreamConfig(streamIdentifier.get(),
                        InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition())));
            }
        }
        return streamConfigList;
    }

    private Optional<StreamIdentifier> getSpecificStreamIdentifier(final String streamName, List<StreamIdentifier> streamIdentifiers) {
        return streamIdentifiers.stream().filter(streamIdentifier -> streamIdentifier.streamName().equals(streamName)).findAny();
    }

    // Implementation reference: https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-list-streams.html
    private List<StreamIdentifier> getAllStreamIdentifiers() {
        ListStreamsRequest listStreamRequest = ListStreamsRequest.builder().build();
        List<StreamSummary> allStreamSummaries = new ArrayList<>();
        ListStreamsResponse listStreamsResult = null;
        do {
            listStreamsResult = kinesisClient.listStreams(listStreamRequest).join();
            allStreamSummaries.addAll(listStreamsResult.streamSummaries());
            List<String> streamNames = listStreamsResult.streamNames();
            listStreamRequest = ListStreamsRequest.builder()
                    .exclusiveStartStreamName(streamNames.get(streamNames.size() - 1))
                    .build();
        } while (listStreamsResult.hasMoreStreams());

        List<StreamIdentifier> streamIdentifiers = new ArrayList<>();
        allStreamSummaries.forEach(streamSummary -> {
            streamIdentifiers.add(StreamIdentifier.multiStreamInstance(getStreamIdentifierString(streamSummary)));
        });
        return streamIdentifiers;
    }

    private String getStreamIdentifierString(StreamSummary streamSummary) {
        String accountId = Arn.fromString(streamSummary.streamARN()).getAccountId();
        long creationEpochSecond = streamSummary.streamCreationTimestamp().getEpochSecond();
        return String.join(COLON, accountId, streamSummary.streamName(), String.valueOf(creationEpochSecond));
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
