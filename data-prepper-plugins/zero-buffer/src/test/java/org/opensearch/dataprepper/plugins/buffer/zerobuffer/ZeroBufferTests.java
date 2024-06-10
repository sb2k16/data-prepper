/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.zerobuffer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class ZeroBufferTests {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String ATTRIBUTE_BUFFER_SIZE = "buffer_size";
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final String PLUGIN_NAME = "ZeroBuffer";
    private static final int TEST_BATCH_SIZE = 3;
    private static final int TEST_BUFFER_SIZE = 13;
    private static final int TEST_WRITE_TIMEOUT = 10;
    private static final int TEST_BATCH_READ_TIMEOUT = 500;
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private Pipeline mockPipeline;

    @BeforeEach
    public void setup() {
        Metrics.globalRegistry.getRegistries().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.addRegistry(new SimpleMeterRegistry());
        mockPipeline = mock(Pipeline.class);
    }

    @Test
    public void testCreationUsingPluginSetting() {
        final PluginSetting completePluginSetting = completePluginSettingForzeroBuffer();
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(completePluginSetting);
        zeroBuffer.setPipeline(mockPipeline);
        assertThat(zeroBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingNullPluginSetting() {
        try {
            new ZeroBuffer<Record<String>>((PluginSetting) null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PluginSetting cannot be null")));
        }
    }

    @Test
    public void testCreationUsingDefaultPluginSettings() {
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(
                ZeroBuffer.getDefaultPluginSettings());
        zeroBuffer.setPipeline(mockPipeline);
        assertThat(zeroBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingValues() {
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(TEST_PIPELINE_NAME);
        zeroBuffer.setPipeline(mockPipeline);

        assertThat(zeroBuffer, notNullValue());
    }

    @Test
    public void testInsertNull() throws TimeoutException {
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(TEST_PIPELINE_NAME);
        zeroBuffer.setPipeline(mockPipeline);

        assertThat(zeroBuffer, notNullValue());
        assertThrows(NullPointerException.class, () -> zeroBuffer.write(null, TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testReadEmptyBuffer() {
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(TEST_PIPELINE_NAME);
        zeroBuffer.setPipeline(mockPipeline);

        assertThat(zeroBuffer, notNullValue());
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = zeroBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(readResult.getKey().size(), is(0));
    }

    @Test
    public void testBufferIsEmpty() {
        final PluginSetting completePluginSetting = completePluginSettingForzeroBuffer();
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(completePluginSetting);
        zeroBuffer.setPipeline(mockPipeline);

        assertTrue(zeroBuffer.isEmpty());
    }

    @Test
    public void testBufferIsNotEmpty() {
        final PluginSetting completePluginSetting = completePluginSettingForzeroBuffer();
        doNothing().when(mockPipeline).executeAllProcessorsAndSinks();

        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(completePluginSetting);
        zeroBuffer.setPipeline(mockPipeline);

        Record<String> record = new Record<>("TEST");
        zeroBuffer.write(record, TEST_WRITE_TIMEOUT);

        assertFalse(zeroBuffer.isEmpty());
    }

    @Test
    void testNonZeroBatchDelayReturnsAllRecords() throws Exception {
        final PluginSetting completePluginSetting = completePluginSettingForzeroBuffer();
        final ZeroBuffer<Record<String>> zeroBuffer = new ZeroBuffer<>(completePluginSetting);
        zeroBuffer.setPipeline(mockPipeline);

        assertThat(zeroBuffer, notNullValue());

        final Collection<Record<String>> testRecords = generateBatchRecords(1);
        zeroBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = zeroBuffer.read(TEST_BATCH_READ_TIMEOUT);
        final Collection<Record<String>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        assertThat(records.size(), is(testRecords.size()));
        assertThat(checkpointState.getNumRecordsToBeChecked(), is(testRecords.size()));
    }

    private PluginSetting completePluginSettingForzeroBuffer() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(ATTRIBUTE_BUFFER_SIZE, TEST_BUFFER_SIZE);
        settings.put(ATTRIBUTE_BATCH_SIZE, TEST_BATCH_SIZE);
        final PluginSetting testSettings = new PluginSetting(PLUGIN_NAME, settings);
        testSettings.setPipelineName(TEST_PIPELINE_NAME);
        return testSettings;
    }

    private Collection<Record<String>> generateBatchRecords(final int numRecords) {
        final Collection<Record<String>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            results.add(new Record<>(UUID.randomUUID().toString()));
        }
        return results;
    }
}
