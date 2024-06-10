package org.opensearch.dataprepper.pipeline.zerobuffer;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.Pipeline;

public abstract class AbstractZeroBuffer <T extends Record<?>> implements Buffer<T> {
    private Pipeline pipeline;

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    @Override
    public boolean isZeroBuffer() {
        return true;
    }
}
