package org.opensearch.dataprepper.plugins.source.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import software.amazon.kinesis.common.InitialPositionInStream;


@Getter
public class KinesisStreamConfig {

    @JsonProperty("stream_name")
    @NotNull
    @Valid
    private String name;

    @JsonProperty("stream_arn")
    private String arn;

    @JsonProperty("kms_key")
    private String kmsKey;

    @JsonProperty("initial_position")
    private InitialPositionInStream initialPosition = InitialPositionInStream.LATEST;


    @JsonProperty("consumer_strategy")
    private ConsumerStrategy consumerStrategy = ConsumerStrategy.POLLING;

    enum ConsumerStrategy {

        POLLING("Polling"),

        ENHANCED_FAN_OUT("Fan-Out");

        private final String value;

        ConsumerStrategy(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }
    }
}
