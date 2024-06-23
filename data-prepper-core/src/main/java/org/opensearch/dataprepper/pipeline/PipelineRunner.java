package org.opensearch.dataprepper.pipeline;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.pipeline.common.FutureHelperResult;
import org.opensearch.dataprepper.pipeline.exceptions.InvalidEventHandleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class PipelineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private final Pipeline pipeline;
    private boolean isEmptyRecordsLogged = false;

    public PipelineRunner(@Nonnull final Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public void runProcessorsAndPublishToSinks(final List<Processor> processors) {
        final boolean acknowledgementsEnabled = pipeline.isAcknowledgementsEnabled();
        final Map.Entry<Collection, CheckpointState> readResult = pipeline.getBuffer().read(pipeline.getReadBatchTimeoutInMillis());
        Collection records = readResult.getKey();
        final CheckpointState checkpointState = readResult.getValue();
        //TODO Hacky way to avoid logging continuously - Will be removed as part of metrics implementation
        if (records.isEmpty()) {
            if (!isEmptyRecordsLogged) {
                LOG.debug(" {} Worker: No records received from buffer", pipeline.getName());
                isEmptyRecordsLogged = true;
            }
        } else {
            LOG.debug(" {} Worker: Processing {} records from buffer", pipeline.getName(), records.size());
        }

        //Should Empty list from buffer should be sent to the processors? For now sending as the Stateful processors expects it.
        records = runProcessorsAndHandleAcknowledgements(processors, records, acknowledgementsEnabled);

        postToSink(records);
        // Checkpoint the current batch read from the buffer after being processed by processors and sinks.
        pipeline.getBuffer().checkpoint(checkpointState);
    }

    public void runAllProcessorsAndPublishToSinks() {
        List<Processor> processors = pipeline.getProcessorSets().stream().flatMap(Collection::stream).collect(Collectors.toList());
        runProcessorsAndPublishToSinks(processors);
    }

    private Collection runProcessorsAndHandleAcknowledgements(List<Processor> processors, Collection records, final boolean acknowledgementsEnabled) {
        for (final Processor processor : processors) {
            List<Event> inputEvents = null;
            if (acknowledgementsEnabled) {
                inputEvents = ((List<Record<Event>>) records).stream().map(Record::getData).collect(Collectors.toList());
            }

            try {
                records = processor.execute(records);
                if (inputEvents != null) {
                    processAcknowledgements(inputEvents, records);
                }
            } catch (final Exception e) {
                LOG.error("A processor threw an exception. This batch of Events will be dropped, and their EventHandles will be released: ", e);
                if (inputEvents != null) {
                    processAcknowledgements(inputEvents, Collections.emptyList());
                }

                records = Collections.emptyList();
                break;
            }
        }
        return records;
    }

    private void processAcknowledgements(List<Event> inputEvents, Collection<Record<Event>> outputRecords) {
        Set<Event> outputEventsSet = outputRecords.stream().map(Record::getData).collect(Collectors.toSet());
        // For each event in the input events list that is not present in the output events, send positive acknowledgement, if acknowledgements are enabled for it
        inputEvents.forEach(event -> {
            EventHandle eventHandle = event.getEventHandle();
            if (eventHandle instanceof DefaultEventHandle) {
                InternalEventHandle internalEventHandle = (InternalEventHandle)(DefaultEventHandle)eventHandle;
                if (!outputEventsSet.contains(event)) {
                    eventHandle.release(true);
                }
            } else if (eventHandle != null) {
                throw new InvalidEventHandleException("Unexpected EventHandle");
            }
        });
    }

    /**
     * TODO Add isolator pattern - Fail if one of the Sink fails [isolator Pattern]
     * Uses the pipeline method to publish to sinks, waits for each of the sink result to be true before attempting to
     * process more records from buffer.
     */
    private boolean postToSink(final Collection<Record> records) {
        LOG.debug("Pipeline Worker: Submitting {} processed records to sinks", records.size());
        final List<Future<Void>> sinkFutures = pipeline.publishToSinks(records);
        final FutureHelperResult<Void> futureResults = FutureHelper.awaitFuturesIndefinitely(sinkFutures);
        return futureResults.getFailedReasons().isEmpty();
    }

}
