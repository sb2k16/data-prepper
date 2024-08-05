package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class JoinAggregateActionConfig {
    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    @JsonProperty("streams")
    @NotEmpty
    private List<String>  streams;

    @JsonProperty("primary_key")
    @NotEmpty
    private String primary_key;

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }

    public List<String> getStreams() {
        return streams;
    }

    public String getPrimary_key() {
        return primary_key;
    }
}
