/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.kinesis.stream;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.kinesis.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.kinesis.utils.KinesisSourceAggregateMetrics;
import org.opensearch.dataprepper.plugins.source.kinesis.utils.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisRequestsBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Factory class to create shard consumers
 */
public class ShardConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumerFactory.class);


    private final KinesisAsyncClient streamsClient;

    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final KinesisSourceAggregateMetrics kinesisSourceAggregateMetrics;
    private final PluginMetrics pluginMetrics;
    private final Buffer<Record<Event>> buffer;
    private final KinesisStreamConfig streamConfig;

    public ShardConsumerFactory(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                final KinesisAsyncClient streamsClient,
                                final PluginMetrics pluginMetrics,
                                final KinesisSourceAggregateMetrics kinesisSourceAggregateMetrics,
                                final Buffer<Record<Event>> buffer,
                                final KinesisStreamConfig streamConfig) {
        this.streamsClient = streamsClient;
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.kinesisSourceAggregateMetrics = kinesisSourceAggregateMetrics;
        this.pluginMetrics = pluginMetrics;
        this.buffer = buffer;
        this.streamConfig = streamConfig;
    }

    public Runnable createConsumer(final StreamPartition streamPartition,
                                   final AcknowledgementSet acknowledgementSet,
                                   final Duration shardAcknowledgmentTimeout) {

        LOG.info("Starting to consume shard " + streamPartition.getShardId());

        // Check and get the current state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        String sequenceNumber = null;
        String lastShardIterator = null;
        Instant startTime = null;
        boolean waitForExport = false;
        if (progressState.isPresent()) {
            // We can't checkpoint with acks yet
            sequenceNumber = acknowledgementSet == null ? null : progressState.get().getSequenceNumber();
            waitForExport = progressState.get().shouldWaitForExport();
            if (progressState.get().getStartTime() != 0) {
                startTime = Instant.ofEpochMilli(progressState.get().getStartTime());
            }
            // If ending sequence number is present, get the shardIterator for last record
            String endingSequenceNumber = progressState.get().getEndingSequenceNumber();
            if (endingSequenceNumber != null && !endingSequenceNumber.isEmpty()) {
                lastShardIterator = getShardIterator(streamPartition.getStreamArn(), streamPartition.getShardId(), endingSequenceNumber);
            }
        }

        String shardIterator = getShardIterator(streamPartition.getStreamArn(), streamPartition.getShardId(), sequenceNumber);
        if (shardIterator == null) {
            LOG.error("Failed to start consuming shard '{}'. Unable to get a shard iterator for this shard, this shard may have expired", streamPartition.getShardId());
            return null;
        }

        StreamCheckpointer checkpointer = new StreamCheckpointer(enhancedSourceCoordinator, streamPartition);
        String tableArn = TableUtil.getTableArnFromStreamArn(streamPartition.getStreamArn());

        LOG.debug("Create shard consumer for {} with shardIter {}", streamPartition.getShardId(), shardIterator);
        LOG.debug("Create shard consumer for {} with lastShardIter {}", streamPartition.getShardId(), lastShardIterator);
        ShardConsumer shardConsumer = ShardConsumer.builder(streamsClient, pluginMetrics, kinesisSourceAggregateMetrics, buffer, streamConfig)
                //.tableInfo(tableInfo)
                .checkpointer(checkpointer)
                .shardIterator(shardIterator)
                .shardId(streamPartition.getShardId())
                .lastShardIterator(lastShardIterator)
                .startTime(startTime)
                .waitForExport(waitForExport)
                .acknowledgmentSet(acknowledgementSet)
                .acknowledgmentSetTimeout(shardAcknowledgmentTimeout)
                .build();
        return shardConsumer;
    }

    /**
     * Get a shard iterator to start reading stream records from a shard.
     * If sequence number is provided, use AT_SEQUENCE_NUMBER to retrieve the iterator,
     * otherwise use TRIM_HORIZON to retrieve the iterator.
     * <p>
     * Note that the shard may be expired, if so, null will be returned.
     * </p>
     *
     * @param streamArn      Stream Arn
     * @param shardId        Shard Id
     * @param sequenceNumber The last Sequence Number processed if any
     * @return A shard iterator.
     */
    public String getShardIterator(String streamArn, String shardId, String sequenceNumber) {
        LOG.debug("Get Initial Shard Iter for {}", shardId);
        GetShardIteratorRequest getShardIteratorRequest;

        if (sequenceNumber != null && !sequenceNumber.isEmpty()) {
            LOG.debug("Get Shard Iterator at {}", sequenceNumber);
            // There may be an overlap for 1 record
            getShardIteratorRequest = KinesisRequestsBuilder.getShardIteratorRequestBuilder()
                    .shardId(shardId)
                    .streamARN(streamArn)
                    .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                    .startingSequenceNumber(sequenceNumber)
                    .build();
        } else {
            LOG.debug("Get Shard Iterator from beginning (TRIM_HORIZON) for shard {}", shardId);
            getShardIteratorRequest = KinesisRequestsBuilder.getShardIteratorRequestBuilder()
                    .shardId(shardId)
                    .streamARN(streamArn)
                    .shardIteratorType(ShardIteratorType.LATEST)
                    .build();
        }

        try {
            kinesisSourceAggregateMetrics.getStreamApiInvocations().increment();
            CompletableFuture<GetShardIteratorResponse> getShardIteratorResult = streamsClient.getShardIterator(getShardIteratorRequest);
            return getShardIteratorResult.get().shardIterator();
        } catch (SdkException e) {
            kinesisSourceAggregateMetrics.getStream4xxErrors().increment();
            LOG.error("Exception when trying to get the shard iterator due to {}", e.getMessage());
            return null;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
