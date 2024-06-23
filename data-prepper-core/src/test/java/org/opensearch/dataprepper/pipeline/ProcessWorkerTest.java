package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.pipeline.exceptions.InvalidEventHandleException;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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

    private ProcessWorker createObjectUnderTest() {
        return new ProcessWorker(buffer, processors, pipeline, pipelineRunner);
    }

    @Test
    void testProcessWorkerPipelineStopRequestedStatus() {

        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));

        final Processor processor = mock(Processor.class);
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
    }

    @Test
    void testProcessWorkerBufferEmptyStatus() {
        when(pipeline.isStopRequested()).thenReturn(true);
        when(buffer.isEmpty()).thenReturn(false).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));

        final Processor processor = mock(Processor.class);
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
    }

    @Test
    void testProcessWorkerProcessorShutdownStatus() {
        when(pipeline.isStopRequested()).thenReturn(true);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(0));

        final Processor processor = mock(Processor.class);
        when(processor.isReadyForShutdown()).thenReturn(false).thenReturn(true);
        processors = List.of(processor);

        doNothing().when(pipelineRunner).runProcessorsAndPublishToSinks(processors);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
    }

    @Test
    void testProcessWorkerNoInteractionPipelineRunner() {
        when(pipeline.isStopRequested()).thenReturn(true);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(0));

        final Processor processor = mock(Processor.class);
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, times(0)).runProcessorsAndPublishToSinks(processors);
    }

    @Test
    void testProcessWorkerWithPipelineRunnerThrowingException() {
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        final Processor processor = mock(Processor.class);
        processors = List.of(processor);

        doThrow(new InvalidEventHandleException("")).when(pipelineRunner).runProcessorsAndPublishToSinks(processors);

        final ProcessWorker processWorker = createObjectUnderTest();

        assertDoesNotThrow(processWorker::run);

        verify(pipelineRunner, atLeastOnce()).runProcessorsAndPublishToSinks(processors);
    }
}