package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class GroupByAggregateActionConfig {
    @JsonProperty("identification_keys")
    @NotEmpty
    private List<String> identificationKeys;

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }
}
