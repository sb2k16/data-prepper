/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.buffer;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@DataPrepperPlugin(name = "synchronized_buffer", pluginType = Buffer.class)
public class SynchronizedBuffer<T extends Record<?>> extends AbstractSynchronizedBuffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SynchronizedBuffer.class);
    private static final String SYNCHRONIZED_BUFFER = "SynchronizedBuffer";
    private final String pipelineName;
    private final ThreadLocal<Collection<T>> threadLocalStore;
    private final Counter recordsWrittenCounter;
    private final Counter recordsReadCounter;

    public SynchronizedBuffer(final String pipelineName) {
        this.pipelineName = pipelineName;
        this.threadLocalStore = new ThreadLocal<>();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(SYNCHRONIZED_BUFFER, pipelineName);
        this.recordsWrittenCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.recordsReadCounter = pluginMetrics.counter(MetricNames.RECORDS_READ);
    }

    public SynchronizedBuffer(final PluginSetting pluginSetting) {
        this(checkNotNull(pluginSetting, "PluginSetting cannot be null").getPipelineName());
    }

    @Override
    public void write(T record, int timeoutInMillis) {
        if (record == null) {
            throw new NullPointerException();
        }

        if (threadLocalStore.get() == null) {
            threadLocalStore.set(new ArrayList<>());
        }
        threadLocalStore.get().add(record);
        getPipelineRunner().runAllProcessorsAndPublishToSinks();
    }

    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        threadLocalStore.set(records);
        getPipelineRunner().runAllProcessorsAndPublishToSinks();
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        if (threadLocalStore.get() == null) {
            threadLocalStore.set(new ArrayList<>());
        }

        Collection<T> records = threadLocalStore.get();
        threadLocalStore.remove();
        final CheckpointState checkpointState = new CheckpointState(records.size());
        recordsReadCounter.increment(records.size() * 1.0);
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    @Override
    public void checkpoint(CheckpointState checkpointState) {

    }

    @Override
    public boolean isEmpty() {
        return (threadLocalStore.get() == null || threadLocalStore.get().isEmpty());
    }
}