/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.KinesisClientApiHandler;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.KinesisRecordConverter;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisStreamNotFoundException;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;


public class KinesisShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Buffer<Record<Event>> buffer;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginMetrics pluginMetrics;
    private final KinesisRecordConverter kinesisRecordConverter;
    private final KinesisClientApiHandler kinesisClientApiHandler;

    public KinesisShardRecordProcessorFactory(Buffer<Record<Event>> buffer,
                                              KinesisSourceConfig kinesisSourceConfig,
                                              final AcknowledgementSetManager acknowledgementSetManager,
                                              final PluginMetrics pluginMetrics,
                                              final InputCodec codec,
                                              final KinesisClientApiHandler kinesisClientApiHandler) {
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.buffer = buffer;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.kinesisClientApiHandler = kinesisClientApiHandler;
        this.kinesisRecordConverter = new KinesisRecordConverter(codec);
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        throw new UnsupportedOperationException("Use the method with stream details!");
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
        BufferAccumulator<Record<Event>> bufferAccumulator = createBufferAccumulator();
        KinesisCheckpointerTracker kinesisCheckpointerTracker = new KinesisCheckpointerTracker();
        KinesisStreamConfig streamConfig = findStreamConfig(streamIdentifier);

        return new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig, acknowledgementSetManager,
                pluginMetrics, kinesisRecordConverter, kinesisCheckpointerTracker, streamIdentifier, streamConfig);
    }

    private BufferAccumulator<Record<Event>> createBufferAccumulator() {
        return BufferAccumulator.create(buffer,
                kinesisSourceConfig.getNumberOfRecordsToAccumulate(),
                kinesisSourceConfig.getBufferTimeout());
    }

    private KinesisStreamConfig findStreamConfig(StreamIdentifier streamIdentifier) {
        String streamIdentifierString = createStreamIdentifierString(streamIdentifier);
        String streamInfo = getStreamInfo(streamIdentifierString, streamIdentifier.streamName());

        return kinesisSourceConfig.getStreams().stream()
                .filter(config -> matchesStreamConfig(config, streamInfo))
                .findFirst()
                .orElseThrow(() -> new KinesisStreamNotFoundException("Kinesis stream not found for " + streamIdentifier.streamName()));
    }

    private String createStreamIdentifierString(StreamIdentifier streamIdentifier) {
        return kinesisClientApiHandler.getStreamIdentifierString(
                streamIdentifier.accountIdOptional().orElseThrow(),
                streamIdentifier.streamName(),
                streamIdentifier.streamCreationEpochOptional().orElseThrow()
        );
    }

    private String getStreamInfo(String streamIdentifierString, String streamName) {
        return kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(streamIdentifierString)
                .orElseThrow(() -> new KinesisStreamNotFoundException("Kinesis stream not found for " + streamName));
    }

    private boolean matchesStreamConfig(KinesisStreamConfig config, String streamInfo) {
        return (config.getArn() != null && config.getArn().equals(streamInfo)) ||
                (config.getName() != null && config.getName().equals(streamInfo));
    }
}