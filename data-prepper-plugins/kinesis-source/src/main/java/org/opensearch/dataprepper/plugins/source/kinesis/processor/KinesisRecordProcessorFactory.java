package org.opensearch.dataprepper.plugins.source.kinesis.processor;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Buffer<Record<Event>> buffer;
    private final InputCodec codec;

    public KinesisRecordProcessorFactory(Buffer<Record<Event>> buffer, InputCodec codec) {
        this.buffer = buffer;
        this.codec = codec;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new KinesisRecordProcessor(buffer, codec);
    }
}