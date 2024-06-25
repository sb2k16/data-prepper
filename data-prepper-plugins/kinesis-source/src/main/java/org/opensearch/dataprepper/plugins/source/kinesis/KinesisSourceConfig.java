package org.opensearch.dataprepper.plugins.source.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.List;


public class KinesisSourceConfig {

    @Getter
    @JsonProperty("streams")
    @NotNull
    @Valid
    private List<KinesisStreamConfig> streams;

    @Getter
    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Getter
    @JsonProperty("codec")
    private PluginModel codec;
}