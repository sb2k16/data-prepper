package org.opensearch.dataprepper.plugins.source.kinesis.processor;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.kinesis.KinesisSourceConfig;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Buffer<Record<Event>> buffer;
    private final InputCodec codec;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginMetrics pluginMetrics;

    public KinesisShardRecordProcessorFactory(Buffer<Record<Event>> buffer,
                                         InputCodec codec, 
                                         KinesisSourceConfig kinesisSourceConfig, 
                                         final AcknowledgementSetManager acknowledgementSetManager, 
                                         final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.codec = codec;
        this.kinesisSourceConfig = kinesisSourceConfig;

        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new KinesisRecordProcessor(buffer, codec, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics);
    }
}