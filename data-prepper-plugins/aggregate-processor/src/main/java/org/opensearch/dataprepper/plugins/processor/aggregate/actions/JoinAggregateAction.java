package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "join", pluginType = AggregateAction.class, pluginConfigurationType = JoinAggregateActionConfig.class)
public class JoinAggregateAction implements AggregateAction {
    private static final String JOINED_KEY = "JOINED_KEY";
    private static final String STREAMS_KEY = "STREAMS_KEY";
    private final IdentificationKeysHasher identificationKeysHasher;
    private final Random random;
    private final JoinAggregateActionConfig joinAggregateActionConfig;

    @DataPrepperPluginConstructor
    public JoinAggregateAction(final JoinAggregateActionConfig joinAggregateActionConfig) {
        this.identificationKeysHasher = new IdentificationKeysHasher(joinAggregateActionConfig.getIdentificationKeys());
        this.joinAggregateActionConfig = joinAggregateActionConfig;
        this.random = new Random();
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();

        String stream = event.get("stream", String.class);
        Map<String, List<Object>> streamMap = (Map<String, List<Object>>) groupState.getOrDefault(stream, new HashMap<>());

        IdentificationKeysHasher.IdentificationKeysMap keysMap = identificationKeysHasher.createIdentificationKeysMapFromEvent(event);
        streamMap.computeIfAbsent(keysMap.toString(), k -> new ArrayList<>()).add(event);

        groupState.put(stream, streamMap);

        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();

        List<Map<String, List<Object>>> listOfAllStreams = new ArrayList<>();
        for (String stream: joinAggregateActionConfig.getStreams()) {
            Map<String, List<Object>> streamMap = (Map<String, List<Object>>) groupState.getOrDefault(stream, new HashMap<>());
            if (streamMap.isEmpty()) {
                return new AggregateActionOutput(List.of());
            }
            listOfAllStreams.add(streamMap);
        }


        List<Event> events = new ArrayList<>();
        // Iterate over all streams
        Map<String, List<List<Object>>> aggregatedMap = new HashMap<>();
        for (Map<String, List<Object>> map : listOfAllStreams) {
            for (Map.Entry<String, List<Object>> entry : map.entrySet()) {
                aggregatedMap.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
            }
        }

        Map<String, List<Object>> mergedJoinedMap = aggregatedMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> mergeAndJoin(entry.getValue())
                ));

        for (Map.Entry<String, List<Object>> key: mergedJoinedMap.entrySet()) {
            List<Object> records = key.getValue();
            records.forEach(record -> events.add((Event) record));
        }

        return new AggregateActionOutput(events);
    }

    private static List<Object> mergeAndJoin(List<List<Object>> lists) {
        if (lists.isEmpty()) {
            return Collections.emptyList();
        }

        // Start with the first list
        List<Object> result = new ArrayList<>(lists.get(0));

        // Compute the Cartesian product for all lists
        for (int i = 1; i < lists.size(); i++) {
            List<Object> currentList = lists.get(i);
            result = result.stream()
                    .flatMap(v1 -> currentList.stream()
                            .map(v2 -> {
                                Event e1 = (Event) v1;
                                Event e2 = (Event) v2;
                                Map<String, Object> m1 = e1.toMap();
                                Map<String, Object> m2 = e2.toMap();
                                m1.putAll(m2);
                                return JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(m1).build();
                            }))
                            .collect(Collectors.toList());
        }

        return result;
    }
}
