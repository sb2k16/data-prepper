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

import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class KinesisMultiStreamTracker implements MultiStreamTracker {
    private static final Duration FORMER_STREAMS_LEASE_DELETION_WAIT_PERIOD = Duration.ofSeconds(10);
    private final KinesisSourceConfig sourceConfig;
    private final String applicationName;
    private final KinesisClientApiHandler kinesisClientAPIHandler;

    public KinesisMultiStreamTracker(final KinesisSourceConfig sourceConfig, final String applicationName, final KinesisClientApiHandler kinesisClientAPIHandler) {
        this.sourceConfig = sourceConfig;
        this.applicationName = applicationName;
        this.kinesisClientAPIHandler = kinesisClientAPIHandler;
    }

    @Override
    public List<StreamConfig> streamConfigList() {
        return sourceConfig.getStreams().stream()
                .map(this::createStreamConfig)
                .collect(Collectors.toList());
    }

    private StreamConfig createStreamConfig(KinesisStreamConfig kinesisStreamConfig) {
        StreamIdentifier streamIdentifier = getStreamIdentifier(kinesisStreamConfig);
        if (Objects.nonNull(kinesisStreamConfig.getArn())) {
            Optional<String> consumerArn = kinesisClientAPIHandler.getConsumerArnForStream(kinesisStreamConfig.getArn(), this.applicationName);
            if (consumerArn.isPresent()) {
                return new StreamConfig(streamIdentifier, InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition()), consumerArn.get());
            }
        }
        return new StreamConfig(streamIdentifier,
                InitialPositionInStreamExtended.newInitialPosition(kinesisStreamConfig.getInitialPosition())
        );
    }

    private StreamIdentifier getStreamIdentifier(KinesisStreamConfig kinesisStreamConfig) {
        if (Objects.nonNull(kinesisStreamConfig.getArn())) {
            return kinesisClientAPIHandler.getStreamIdentifierFromStreamArn(kinesisStreamConfig.getArn());
        } else if (Objects.nonNull(kinesisStreamConfig.getName())) {
            return kinesisClientAPIHandler.getStreamIdentifier(kinesisStreamConfig.getName());
        } else {
            throw new IllegalArgumentException("Either ARN or name must be specified for Kinesis stream configuration");
        }
    }

    /**
     * Setting the deletion policy as autodetect and release shard lease with a wait time of 10 sec
     */
    @Override
    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
        return new FormerStreamsLeasesDeletionStrategy.AutoDetectionAndDeferredDeletionStrategy() {
            @Override
            public Duration waitPeriodToDeleteFormerStreams() {
                return FORMER_STREAMS_LEASE_DELETION_WAIT_PERIOD;
            }
        };
    }
}
