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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.KinesisClientApiHandler;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisStreamNotFoundException;
import software.amazon.kinesis.common.StreamIdentifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisShardRecordProcessorFactoryTest {
    private KinesisShardRecordProcessorFactory kinesisShardRecordProcessorFactory;

    private static final String streamId = "stream-1";
    private static final String codec_plugin_name = "json";

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    StreamIdentifier streamIdentifier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private KinesisStreamConfig kinesisStreamConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private KinesisClientApiHandler kinesisClientApiHandler;

    @Mock
    private InputCodec codec;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(100);

        codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);

        when(streamIdentifier.streamName()).thenReturn(streamId);
        final String accountID = RandomStringUtils.randomNumeric(12);
        when(streamIdentifier.accountIdOptional()).thenReturn(Optional.of(accountID));
        when(streamIdentifier.streamCreationEpochOptional()).thenReturn(Optional.of(1L));
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
        when(kinesisClientApiHandler.getStreamIdentifierString(any(), any(), anyLong())).thenReturn(streamId);
        when(kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(any())).thenReturn(Optional.of(streamId));
    }

    @AfterEach
    void tearDown() throws Exception {
        AutoCloseable closeable = MockitoAnnotations.openMocks(this);
        closeable.close();
    }

    @Test
    void testKinesisRecordProcessFactoryReturnsKinesisRecordProcessor() {
        kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec, kinesisClientApiHandler);
        assertInstanceOf(KinesisRecordProcessor.class, kinesisShardRecordProcessorFactory.shardRecordProcessor(streamIdentifier));
    }

    @Test
    void testKinesisRecordProcessFactoryDefaultUnsupported() {
        kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec, kinesisClientApiHandler);
        assertThrows(UnsupportedOperationException.class, () -> kinesisShardRecordProcessorFactory.shardRecordProcessor());
    }

    @Test
    void testShardRecordProcessor_streamNotFound_throwsException() {
        when(streamIdentifier.streamName()).thenReturn("nonExistentStream");
        final String accountID = RandomStringUtils.randomNumeric(12);
        when(streamIdentifier.accountIdOptional()).thenReturn(Optional.of(accountID));
        when(streamIdentifier.streamCreationEpochOptional()).thenReturn(Optional.of(1L));
        when(kinesisClientApiHandler.getStreamIdentifierString(any(), any(), anyLong())).thenReturn("testStreamIdentifier");
        when(kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(any())).thenReturn(Optional.of("testStreamInfo"));
        when(kinesisSourceConfig.getStreams()).thenReturn(Collections.emptyList());

        KinesisShardRecordProcessorFactory kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec, kinesisClientApiHandler);

        KinesisStreamNotFoundException exception = assertThrows(KinesisStreamNotFoundException.class,
                () -> kinesisShardRecordProcessorFactory.shardRecordProcessor(streamIdentifier));
        assertThat(exception.getMessage(), containsString("nonExistentStream"));
    }

    @Test
    void testShardRecordProcessor_multipleStreams_findsCorrectStream() {
        String targetStreamName = UUID.randomUUID().toString();
        when(streamIdentifier.streamName()).thenReturn(targetStreamName);
        when(kinesisClientApiHandler.getStreamIdentifierString(any(), any(), anyLong())).thenReturn("testStreamIdentifier");
        when(kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(any())).thenReturn(Optional.of(targetStreamName));

        KinesisStreamConfig config1 = mock(KinesisStreamConfig.class);
        when(config1.getName()).thenReturn("stream1");
        KinesisStreamConfig config2 = mock(KinesisStreamConfig.class);
        when(config2.getName()).thenReturn(targetStreamName);
        KinesisStreamConfig config3 = mock(KinesisStreamConfig.class);
        when(config3.getName()).thenReturn("stream3");

        KinesisShardRecordProcessorFactory kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec, kinesisClientApiHandler);

        when(kinesisSourceConfig.getStreams()).thenReturn(Arrays.asList(config1, config2, config3));

        assertDoesNotThrow(() -> kinesisShardRecordProcessorFactory.shardRecordProcessor(streamIdentifier));
    }

    @Test
    void testShardRecordProcessor_streamFoundByArn() {
        String targetStreamArn = "arn:aws:kinesis:us-east-1:123456789012:stream/targetStream";
        when(streamIdentifier.streamName()).thenReturn("targetStream");
        when(kinesisClientApiHandler.getStreamIdentifierString(any(), any(), anyLong())).thenReturn("testStreamIdentifier");
        when(kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(any())).thenReturn(Optional.of(targetStreamArn));

        KinesisStreamConfig config = mock(KinesisStreamConfig.class);
        when(config.getArn()).thenReturn(targetStreamArn);

        when(kinesisSourceConfig.getStreams()).thenReturn(Collections.singletonList(config));

        KinesisShardRecordProcessorFactory kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec, kinesisClientApiHandler);

        assertDoesNotThrow(() -> kinesisShardRecordProcessorFactory.shardRecordProcessor(streamIdentifier));
    }

}
