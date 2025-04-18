package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.KinesisClientApiHandler;
import software.amazon.kinesis.common.StreamIdentifier;

import java.text.MessageFormat;
import java.util.Optional;

@Slf4j
public class StreamARNConfigurator {

    private final KinesisClientApiHandler kinesisClientApiHandler;

    public StreamARNConfigurator(final KinesisClientApiHandler kinesisClientApiHandler) {
        this.kinesisClientApiHandler = kinesisClientApiHandler;
    }

    public String buildKinesisStreamArn(final StreamIdentifier streamIdentifier) {
        final String streamIdentifierString = kinesisClientApiHandler.getStreamIdentifierString(
                streamIdentifier.accountIdOptional().orElseThrow(),
                streamIdentifier.streamName(),
                streamIdentifier.streamCreationEpochOptional().orElseThrow()
        );
        Optional<String> streamArn = kinesisClientApiHandler.getStreamInfoFromStreamIdentifier(streamIdentifierString);
        return streamArn.get();
    }
}