package org.opensearch.dataprepper.plugins.source.kinesis.processor;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.kinesis.KinesisSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordProcessor implements ShardRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);


    // Whether or not to do regular checkpointing.
    // It's a good practice to do regular checkpointing to avoid unknown exception occurred.
    // Otherwise, only when shutdown or shard end will trigger checkpointing
    private static final boolean DEFAULT_REGULAR_CHECKPOINTING = true;


    // Checkpointing interval
    private static final int MINIMAL_CHECKPOINT_INTERVAL_MILLIS = 2 * 60 * 1000; // 2 minute

    private static final int DEFAULT_TIME_OUT_IN_MILLIS = 10000;

    private String kinesisShardId;

    private final Buffer<Record<Event>> buffer;
    private final InputCodec codec;

    private long nextCheckpointTimeInMillis;

    private final boolean doRegularCheckpointing;

    public KinesisRecordProcessor(Buffer<Record<Event>> buffer, InputCodec codec) {
        this.buffer = buffer;
        this.codec = codec;
        doRegularCheckpointing = DEFAULT_REGULAR_CHECKPOINTING;
    }


    @Override
    public void initialize(InitializationInput initializationInput) {
        // Called once when the processor is initialized.
        kinesisShardId = initializationInput.shardId();
        LOG.info("Initialize Processor for shard: " + kinesisShardId);

        nextCheckpointTimeInMillis = System.currentTimeMillis() + MINIMAL_CHECKPOINT_INTERVAL_MILLIS;
    }


    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        LOG.debug("Processing " + processRecordsInput.records().size() + " record(s)");
        List<Record<Event>> records = new ArrayList<>();
        processRecordsInput.records().forEach(r -> {
            try {
                processRecord(r, records::add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            buffer.writeAll(records, DEFAULT_TIME_OUT_IN_MILLIS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Do Regular Checkpointing
        // processRecordsInput also includes a CheckPointer instance
        // Note that this will only be executed when there are data to be processed.
        if (doRegularCheckpointing && System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            LOG.info("Regular checkpointing for shard " + kinesisShardId);
            checkpoint(processRecordsInput.checkpointer());
            nextCheckpointTimeInMillis = System.currentTimeMillis() + MINIMAL_CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void processRecord(KinesisClientRecord record, Consumer<Record<Event>> eventConsumer) throws IOException {
        // Read bytebuffer
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(arr);
        // Invoke codec
        codec.parse(byteArrayInputStream, eventConsumer);
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        LOG.debug("Lease Lost");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        // Important to checkpoint after reaching end of shard
        // So we can start processing data from child shards.
        LOG.info("Reached shard end, checkpointing shard " + kinesisShardId);
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        LOG.info("Scheduler is shutting down, checkpointing shard " + kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }


    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }
}
