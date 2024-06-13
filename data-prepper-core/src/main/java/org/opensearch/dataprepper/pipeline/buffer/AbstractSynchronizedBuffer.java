package org.opensearch.dataprepper.pipeline.buffer;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.PipelineRunner;

public abstract class AbstractSynchronizedBuffer<T extends Record<?>> implements Buffer<T> {
    private PipelineRunner pipelineRunner;

    public PipelineRunner getPipelineRunner() {
        return pipelineRunner;
    }

    public void setPipelineRunner(PipelineRunner pipelineRunner) {
        this.pipelineRunner = pipelineRunner;
    }
}
