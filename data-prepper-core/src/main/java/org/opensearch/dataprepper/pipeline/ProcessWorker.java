/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.pipeline.exceptions.InvalidEventHandleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProcessWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessWorker.class);

    private static final String INVALID_EVENT_HANDLES = "invalidEventHandles";
    private final Buffer readBuffer;
    private final List<Processor> processors;
    private final Pipeline pipeline;
    private final PipelineRunner pipelineRunner;
    private PluginMetrics pluginMetrics;
    private final Counter invalidEventHandlesCounter;

    public ProcessWorker(
            final Buffer readBuffer,
            final List<Processor> processors,
            final Pipeline pipeline,
            final PipelineRunner pipelineRunner) {
        this.readBuffer = readBuffer;
        this.processors = processors;
        this.pipeline = pipeline;
        this.pluginMetrics = PluginMetrics.fromNames("ProcessWorker", pipeline.getName());
        this.invalidEventHandlesCounter = pluginMetrics.counter(INVALID_EVENT_HANDLES);
        this.pipelineRunner = pipelineRunner;
    }

    @Override
    public void run() {
        try {
            // Phase 1 - execute until stop requested
            while (!pipeline.isStopRequested()) {
                doRun();
            }
            LOG.info("Processor shutdown phase 1 complete.");

            // Phase 2 - execute until buffers are empty
            LOG.info("Beginning processor shutdown phase 2, iterating until buffers empty.");
            while (!readBuffer.isEmpty()) {
                doRun();
            }
            LOG.info("Processor shutdown phase 2 complete.");

            // Phase 3 - execute until peer forwarder drain period expires (best effort to process all peer forwarder data)
            final long drainTimeoutExpiration = System.currentTimeMillis() + pipeline.getPeerForwarderDrainTimeout().toMillis();
            LOG.info("Beginning processor shutdown phase 3, iterating until {}.", drainTimeoutExpiration);
            while (System.currentTimeMillis() < drainTimeoutExpiration) {
                doRun();
            }
            LOG.info("Processor shutdown phase 3 complete.");

            // Phase 4 - prepare processors for shutdown
            LOG.info("Beginning processor shutdown phase 4, preparing processors for shutdown.");
            processors.forEach(Processor::prepareForShutdown);
            LOG.info("Processor shutdown phase 4 complete.");

            // Phase 5 - execute until processors are ready to shutdown
            LOG.info("Beginning processor shutdown phase 5, iterating until processors are ready to shutdown.");
            while (!areComponentsReadyForShutdown()) {
                doRun();
            }
            LOG.info("Processor shutdown phase 5 complete.");
        } catch (final Exception e) {
            LOG.error("Encountered exception during pipeline {} processing", pipeline.getName(), e);
        }
    }

    private void doRun() {
        try {
            pipelineRunner.runProcessorsAndPublishToSinks(processors);
        } catch (InvalidEventHandleException ex) {
            invalidEventHandlesCounter.increment();
            throw ex;
        }
    }

    private boolean areComponentsReadyForShutdown() {
        return readBuffer.isEmpty() && processors.stream()
                .map(Processor::isReadyForShutdown)
                .allMatch(result -> result == true);
    }
}
