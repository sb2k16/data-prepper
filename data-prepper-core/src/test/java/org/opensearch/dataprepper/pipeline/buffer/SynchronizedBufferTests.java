/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.buffer;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.PipelineRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SynchronizedBufferTests {
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final int TEST_WRITE_TIMEOUT = 10;
    private static final int TEST_BATCH_READ_TIMEOUT = 500;

    @Mock
    PluginSetting pipelineDescription;

    @Mock
    private PipelineRunner mockPipelineRunner;

    @BeforeEach
    public void setup() {
        Metrics.globalRegistry.getRegistries().forEach(Metrics.globalRegistry::remove);
        Metrics.globalRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
        Metrics.addRegistry(new SimpleMeterRegistry());
        mockPipelineRunner = mock(PipelineRunner.class);
        pipelineDescription = mock(PluginSetting.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    @Test
    public void testCreationUsingPipelineDescription() {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(pipelineDescription);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);
        assertThat(synchronizedBuffer, notNullValue());
    }

    @Test
    public void testCreationUsingNullPipelineDescription() {
        try {
            new SynchronizedBuffer<Record<String>>((PluginSetting) null);
        } catch (NullPointerException ex) {
            assertThat(ex.getMessage(), is(equalTo("PipelineDescription cannot be null")));
        }
    }

    @Test
    public void testCreationUsingValues() {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(TEST_PIPELINE_NAME);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        assertThat(synchronizedBuffer, notNullValue());
    }

    @Test
    public void testInsertNull() {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(TEST_PIPELINE_NAME);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        assertThat(synchronizedBuffer, notNullValue());
        assertThrows(NullPointerException.class, () -> synchronizedBuffer.write(null, TEST_WRITE_TIMEOUT));
    }

    @Test
    public void testReadEmptyBuffer() {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(TEST_PIPELINE_NAME);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        assertThat(synchronizedBuffer, notNullValue());
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = synchronizedBuffer.read(TEST_BATCH_READ_TIMEOUT);
        assertThat(readResult.getKey().size(), is(0));
    }

    @Test
    public void testBufferIsEmpty() {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(pipelineDescription);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        assertTrue(synchronizedBuffer.isEmpty());
    }

    @Test
    public void testBufferIsNotEmpty() {
        doNothing().when(mockPipelineRunner).runAllProcessorsAndPublishToSinks();

        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(pipelineDescription);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        Record<String> record = new Record<>("TEST");
        synchronizedBuffer.write(record, TEST_WRITE_TIMEOUT);

        assertFalse(synchronizedBuffer.isEmpty());
    }

    @Test
    void testNonZeroBatchDelayReturnsAllRecords() throws Exception {
        final SynchronizedBuffer<Record<String>> synchronizedBuffer = new SynchronizedBuffer<>(pipelineDescription);
        synchronizedBuffer.setPipelineRunner(mockPipelineRunner);

        assertThat(synchronizedBuffer, notNullValue());

        final Collection<Record<String>> testRecords = generateBatchRecords();
        synchronizedBuffer.writeAll(testRecords, TEST_WRITE_TIMEOUT);
        final Map.Entry<Collection<Record<String>>, CheckpointState> readResult = synchronizedBuffer.read(TEST_BATCH_READ_TIMEOUT);
        final Collection<Record<String>> records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        assertThat(records.size(), is(testRecords.size()));
        assertThat(checkpointState.getNumRecordsToBeChecked(), is(testRecords.size()));
    }

    private Collection<Record<String>> generateBatchRecords() {
        final Collection<Record<String>> results = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            results.add(new Record<>(UUID.randomUUID().toString()));
        }
        return results;
    }
}
