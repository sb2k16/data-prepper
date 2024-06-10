/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.zerobuffer;

import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.zerobuffer.AbstractZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.micrometer.core.instrument.Counter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@DataPrepperPlugin(name = "zero_buffer", pluginType = Buffer.class)
public class ZeroBuffer<T extends Record<?>> extends AbstractZeroBuffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ZeroBuffer.class);
    private static final String PLUGIN_NAME = "zero_buffer";
    private static final String ZERO_BUFFER = "ZeroBuffer";
    private final String pipelineName;
    private final ThreadLocal<Collection<T>> threadLocalStore;
    private final Counter recordsWrittenCounter;

    public ZeroBuffer(final String pipelineName) {
        this.pipelineName = pipelineName;
        this.threadLocalStore = new ThreadLocal<>();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(ZERO_BUFFER, pipelineName);

        this.recordsWrittenCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
    }

    public ZeroBuffer(final PluginSetting pluginSetting) {
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
        getPipeline().executeAllProcessorsAndSinks();
    }

    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        threadLocalStore.set(records);

        getPipeline().executeAllProcessorsAndSinks();
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        if (threadLocalStore.get() == null) {
            threadLocalStore.set(new ArrayList<>());
        }

        Collection<T> records = threadLocalStore.get();
        threadLocalStore.remove();
        final CheckpointState checkpointState = new CheckpointState(records.size());
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    @Override
    public void checkpoint(CheckpointState checkpointState) {

    }

    /**
     * Returns the default PluginSetting object with default values.
     * @return PluginSetting
     */
    public static PluginSetting getDefaultPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    @Override
    public boolean isEmpty() {
        return (threadLocalStore.get() == null || threadLocalStore.get().isEmpty());
    }

    @Override
    public boolean isZeroBuffer() {
        return true;
    }
}
