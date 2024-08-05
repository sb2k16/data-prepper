package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

@DataPrepperPlugin(name = "groupby", pluginType = AggregateAction.class, pluginConfigurationType = GroupByAggregateActionConfig.class)
public class GroupByAggregateAction implements AggregateAction {
    static final String LAST_RECEIVED_TIME_KEY = "last_received_time";
    static final String SHOULD_CONCLUDE_CHECK_SET_KEY = "should_conclude_check_set";
    static final String EVENTS_KEY = "events";
    static final String ERROR_STATUS_KEY = "error_status";
    private final Random random;
    private final IdentificationKeysHasher identificationKeysHasher;

    @DataPrepperPluginConstructor
    public GroupByAggregateAction(final GroupByAggregateActionConfig groupByAggregateActionConfig) {
        this.identificationKeysHasher = new IdentificationKeysHasher(groupByAggregateActionConfig.getIdentificationKeys());
        this.random = new Random();
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        List<Event> events = (List)groupState.getOrDefault(EVENTS_KEY, new ArrayList<>());
        events.add(event);
        final IdentificationKeysHasher.IdentificationKeysMap identificationKeysMap = identificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        event.getMetadata().setAttribute("partition_key", identificationKeysMap.hashCode());
        groupState.put(EVENTS_KEY, events);
        groupState.put(LAST_RECEIVED_TIME_KEY, Instant.now());
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        return new AggregateActionOutput((List)groupState.getOrDefault(EVENTS_KEY, List.of()));
    }
}
