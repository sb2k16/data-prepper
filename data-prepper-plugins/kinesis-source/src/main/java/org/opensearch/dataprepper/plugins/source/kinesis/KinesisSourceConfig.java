package org.opensearch.dataprepper.plugins.source.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.List;


public class KinesisSourceConfig {

    // Whether or not to do regular checkpointing.
    // It's a good practice to do regular checkpointing to avoid unknown exception occurred.
    // Otherwise, only when shutdown or shard end will trigger checkpointing
    private static final boolean DEFAULT_ENABLE_CHECKPOINT = false;

    private static final Duration DEFAULT_TIME_OUT_IN_MILLIS = Duration.ofMillis(10000);

    @Getter
    @JsonProperty("streams")
    @NotNull
    @Valid
    @Size(min = 1, max = 4, message = "Only support a maximum of 4 streams")
    private List<KinesisStreamConfig> streams;

    @Getter
    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Getter
    @JsonProperty("codec")
    private PluginModel codec;

    @Getter
    @JsonProperty("enable_checkpoint")
    private boolean enableCheckPoint = DEFAULT_ENABLE_CHECKPOINT;

    @Getter
    @JsonProperty("buffer_timeout")
    private Duration bufferTimeout = DEFAULT_TIME_OUT_IN_MILLIS;

    @JsonProperty("acknowledgments")
    @Getter
    private boolean acknowledgments = false;

    @JsonProperty("shard_acknowledgment_timeout")
    private Duration shardAcknowledgmentTimeout = Duration.ofMinutes(10);

    public Duration getShardAcknowledgmentTimeout() {
        return shardAcknowledgmentTimeout;
    }
}

/**
     * Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
     * will be regarded as having problems and it's shards will be assigned to other workers.
     * For applications that have a large number of shards, this may be set to a higher number to reduce
     * the number of DynamoDB IOPS required for tracking leases.
     *
     * <p>Default value: 10000L</p>

private long failoverTimeMillis = 10000L;

 >>> Add Failover time (ms) for lease expiration.
**/



