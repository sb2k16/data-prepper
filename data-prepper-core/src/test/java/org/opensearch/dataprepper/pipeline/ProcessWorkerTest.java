package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.source.Source;

import java.time.Duration;
import java.util.List;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProcessWorkerTest {

    @Mock
    private Pipeline pipeline;

    @Mock
    private Buffer buffer;

    @Mock
    private Source source;

    @Mock
    private PipelineRunner pipelineRunner;

    private List<Processor> processors;

    @BeforeEach
    void setup() {
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(source.areAcknowledgementsEnabled()).thenReturn(false);
        when(pipeline.getSource()).thenReturn(source);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));
    }

    private ProcessWorker createObjectUnderTest() {
        return new ProcessWorker(buffer, processors, pipeline, pipelineRunner);
    }

    @Test
    void testProcessWorkerHappyPath() {

        final Processor processor = mock(Processor.class);
        processors = List.of(processor);
        when(pipeline.isAcknowledgementsEnabled()).thenReturn(false);

        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
    }

//    @Test
//    void testProcessWorkerHappyPathWithSourceAcknowledgments() {
//
//        when(source.areAcknowledgementsEnabled()).thenReturn(true);
//
//        final List<Record<Event>> records = new ArrayList<>();
//        final Record<Event> mockRecord = mock(Record.class);
//        records.add(mockRecord);
//
//        final Processor processor = mock(Processor.class);
//        processors = List.of(processor);
//        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);
//
//        final ProcessWorker processWorker = createObjectUnderTest();
//
//        processWorker.run();
//        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
//    }
//
//    @Test
//    void testProcessWorkerHappyPathWithBufferAcknowledgments() {
//
//        when(buffer.areAcknowledgementsEnabled()).thenReturn(true);
//
//        final List<Record<Event>> records = new ArrayList<>();
//        final Record<Event> mockRecord = mock(Record.class);
//        records.add(mockRecord);
//
//        final Processor processor = mock(Processor.class);
//        processors = List.of(processor);
//
//        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);
//
//        final ProcessWorker processWorker = createObjectUnderTest();
//
//        processWorker.run();
//        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
//    }
//
//    @Test
//    void testProcessWorkerWithPipelineRunnerThrowingException() {
//
//        final Processor processor = mock(Processor.class);
//        final Processor skippedProcessor = mock(Processor.class);
//        when(skippedProcessor.isReadyForShutdown()).thenReturn(true);
//        processors = List.of(processor, skippedProcessor);
//
//        doThrow(new InvalidEventHandleException("")).when(pipelineRunner).runProcessorsAndPublishToSinks(processors);
//
//        final ProcessWorker processWorker = createObjectUnderTest();
//
//        assertThrows(InvalidEventHandleException.class, processWorker::run);
//
//        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
//
//        verify(skippedProcessor, never()).execute(any());
//    }
}