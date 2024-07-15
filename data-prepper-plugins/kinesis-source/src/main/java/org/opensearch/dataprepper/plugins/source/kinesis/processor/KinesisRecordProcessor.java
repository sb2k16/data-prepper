package org.opensearch.dataprepper.plugins.source.kinesis.processor;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerFactory;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.kinesis.KinesisSource;
import org.opensearch.dataprepper.plugins.source.kinesis.KinesisSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordProcessor implements ShardRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

    // Checkpointing interval
    private static final int MINIMAL_CHECKPOINT_INTERVAL_MILLIS = 2 * 60 * 1000; // 2 minute
    private final boolean acknowledgementsEnabled;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private String kinesisShardId;
    private final Buffer<Record<Event>> buffer;
    private final InputCodec codec;
    private long nextCheckpointTimeInMillis;
    private final boolean enableCheckpoint;
    private final int bufferTimeoutMillis;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private static final DataFormat dataFormat = DataFormat.JSON;
    private final GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;
    private final GlueSchemaRegistryDataFormatDeserializer gsrDataFormatDeserializer;
    static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofSeconds(20);
    private final Counter acknowledgementSetCallbackCounter;
    static final String ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    public KinesisRecordProcessor(Buffer<Record<Event>> buffer, InputCodec codec, KinesisSourceConfig kinesisSourceConfig, final AcknowledgementSetManager acknowledgementSetManager, final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.codec = codec;
        this.enableCheckpoint = kinesisSourceConfig.isEnableCheckPoint();
        this.bufferTimeoutMillis = (int) kinesisSourceConfig.getBufferTimeout().toMillis();
        this.acknowledgementSetManager = acknowledgementSetManager;
        GlueSchemaRegistryConfiguration gsrConfig = new GlueSchemaRegistryConfiguration("us-east-1");
        glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializerImpl(kinesisSourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration(), gsrConfig);
        GlueSchemaRegistryDeserializerFactory glueSchemaRegistryDeserializerFactory = new GlueSchemaRegistryDeserializerFactory();
        gsrDataFormatDeserializer = glueSchemaRegistryDeserializerFactory.getInstance(dataFormat, gsrConfig);
        this.acknowledgementsEnabled = kinesisSourceConfig.isAcknowledgments();
        this.bufferAccumulator = BufferAccumulator.create(buffer, 1, Duration.ofSeconds(1));
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME);
    }


    @Override
    public void initialize(InitializationInput initializationInput) {
        // Called once when the processor is initialized.
        kinesisShardId = initializationInput.shardId();
        LOG.info("Initialize Processor for shard: " + kinesisShardId);
        nextCheckpointTimeInMillis = System.currentTimeMillis() + MINIMAL_CHECKPOINT_INTERVAL_MILLIS;
    }

    private AcknowledgementSet createAcknowledgmentSet(final ProcessRecordsInput processRecordsInput) {
        return acknowledgementSetManager.create((result) -> {
            acknowledgementSetCallbackCounter.increment();
            if (result) {
                LOG.info("acknowledgements received");
                checkpoint(processRecordsInput.checkpointer());
            } else {
                LOG.info("acknowledgements received with false");
            }

        }, ACKNOWLEDGEMENT_SET_TIMEOUT);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        LOG.debug("Processing " + processRecordsInput.records().size() + " record(s)");
        List<Record<Event>> records = new ArrayList<>();
        AcknowledgementSet acknowledgementSet;
        if (acknowledgementsEnabled) {
            acknowledgementSet = createAcknowledgmentSet(processRecordsInput);
        } else {
            acknowledgementSet = null;
        }

        processRecordsInput.records().forEach(r -> {
            try {
                processRecord(r, records::add);
            } catch (IOException e) {
                Runtime.getRuntime().halt(1);
            }
        });

        if (acknowledgementSet != null) {
            records.forEach(record -> {
                acknowledgementSet.add(record.getData());
            });
        }
        try {
            buffer.writeAll(records, bufferTimeoutMillis);
        } catch (Exception e) {
            Runtime.getRuntime().halt(1);
        }

        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }

        // Do Regular Checkpointing
        // processRecordsInput also includes a CheckPointer instance
        // Note that this will only be executed when there are data to be processed.
        if (enableCheckpoint && System.currentTimeMillis() > nextCheckpointTimeInMillis) {
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
