package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.pipeline.common.FutureHelperResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineRunnerTests {
    private static final int TEST_READ_BATCH_TIMEOUT = 500;
    List<Future<Void>> sinkFutures;
    private List<Record> records;
    private AcknowledgementSet acknowledgementSet;
    private List<Processor> processors;
    private Buffer mockBuffer;
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private Pipeline mockPipeline;

    @BeforeEach
    void setUp() {
        mockBuffer = mock(Buffer.class);
        processors = IntStream.range(0, 3)
                .mapToObj(i -> mock(Processor.class))
                .collect(Collectors.toList());

        mockPipeline = mock(Pipeline.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);

        when(mockPipeline.getReadBatchTimeoutInMillis()).thenReturn(500);

        final Future<Void> sinkFuture = mock(Future.class);
        sinkFutures = List.of(sinkFuture);
        when(mockPipeline.publishToSinks(any())).thenReturn(sinkFutures);
        when(mockPipeline.getBuffer()).thenReturn(mockBuffer);
    }

    private PipelineRunner createObjectUnderTest() {
        return new PipelineRunner(mockPipeline);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRunProcessorsWithAcknowledgements(final boolean acknowledgementsEnabled) {

        // Prepare
        records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        final Event mockEvent = mock(Event.class);
        if (acknowledgementsEnabled) {
            final EventHandle eventHandle = mock(DefaultEventHandle.class);
            when(((DefaultEventHandle) eventHandle).getAcknowledgementSet()).thenReturn(mock(AcknowledgementSet.class));
            when(mockRecord.getData()).thenReturn(mockEvent);
            when(mockEvent.getEventHandle()).thenReturn(eventHandle);
        }

        when(mockPipeline.isAcknowledgementsEnabled()).thenReturn(acknowledgementsEnabled);

        records.add(mockRecord);
        for (Processor processor : processors) {
            when(processor.execute(records)).thenReturn(records);
            when(processor.isReadyForShutdown()).thenReturn(true);
        }

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(mockBuffer.read(TEST_READ_BATCH_TIMEOUT)).thenReturn(readResult);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());

        // Then
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            PipelineRunner pipelineRunner = createObjectUnderTest();
            pipelineRunner.runProcessorsAndPublishToSinks(processors);

            for (Processor processor : processors) {
                verify(processor).execute(records);
            }

            verify(mockPipeline, times(0)).getProcessorSets();
            verify(mockPipeline, times(1)).publishToSinks(eq(records));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRunProcessorsThrowingExceptionWithAcknowledgements(final boolean acknowledgementsEnabled) {
        // Prepare
        records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        final Event mockEvent = mock(Event.class);
        if (acknowledgementsEnabled) {
            final EventHandle eventHandle = mock(DefaultEventHandle.class);
            when(((DefaultEventHandle) eventHandle).getAcknowledgementSet()).thenReturn(mock(AcknowledgementSet.class));
            when(mockRecord.getData()).thenReturn(mockEvent);
            when(mockEvent.getEventHandle()).thenReturn(eventHandle);
        }
        records.add(mockRecord);
        when(mockPipeline.isAcknowledgementsEnabled()).thenReturn(acknowledgementsEnabled);
        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenThrow(RuntimeException.class);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final Processor skippedProcessor = mock(Processor.class);
        when(skippedProcessor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor, skippedProcessor);

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(mockBuffer.read(TEST_READ_BATCH_TIMEOUT)).thenReturn(readResult);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());

        // Then
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            PipelineRunner pipelineRunner = createObjectUnderTest();
            pipelineRunner.runProcessorsAndPublishToSinks(processors);

            verify(skippedProcessor, never()).execute(any());

            verify(mockPipeline, times(0)).getProcessorSets();
            verify(mockPipeline, times(1)).publishToSinks(anyList());
        }
    }

    @Test
    void testRunProcessorsDroppingRecords() {
        // Prepare
        records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        records.add(mockRecord);
        for (Processor processor : processors) {
            when(processor.execute(records)).thenReturn(Collections.emptyList());
            when(processor.isReadyForShutdown()).thenReturn(true);
        }

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(mockBuffer.read(TEST_READ_BATCH_TIMEOUT)).thenReturn(readResult);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());

        // Then
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            PipelineRunner pipelineRunner = createObjectUnderTest();
            pipelineRunner.runProcessorsAndPublishToSinks(processors);

            for (Processor processor : processors) {
                verify(processor, times(1)).execute(anyList());
            }

            verify(mockPipeline, times(0)).getProcessorSets();
            verify(mockPipeline, times(1)).publishToSinks(anyList());
        }
    }

    @Test
    void testRunAllProcessors() {
        when(mockPipeline.getProcessorSets()).thenReturn(List.of(processors));
        records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        records.add(mockRecord);
        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(mockBuffer.read(TEST_READ_BATCH_TIMEOUT)).thenReturn(readResult);
        for (Processor processor : processors) {
            when(processor.execute(records)).thenReturn(records);
            when(processor.isReadyForShutdown()).thenReturn(true);
        }

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());

        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            PipelineRunner pipelineRunner = createObjectUnderTest();
            pipelineRunner.runAllProcessorsAndPublishToSinks();

            for (Processor processor : processors) {
                verify(processor).execute(records);
            }

            verify(mockPipeline, times(1)).getProcessorSets();
            verify(mockPipeline, times(1)).publishToSinks(eq(records));
        }
    }
}
