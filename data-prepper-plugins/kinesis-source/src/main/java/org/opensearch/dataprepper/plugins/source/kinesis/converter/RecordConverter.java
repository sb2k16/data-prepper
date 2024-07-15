/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.kinesis.converter;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;

import java.math.BigDecimal;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.kinesis.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.kinesis.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;

/**
 * Base Record Processor definition.
 * The record processor is to transform the source data into a JacksonEvent,
 * and then write to buffer.
 */
public abstract class RecordConverter {

    private static final String DEFAULT_ACTION = OpenSearchBulkActions.INDEX.toString();

    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    public RecordConverter(final BufferAccumulator<Record<Event>> bufferAccumulator) {
        this.bufferAccumulator = bufferAccumulator;
    }

    abstract String getEventType();

    /**
     * Extract the value based on attribute map
     *
     * @param data          A map of attribute name and value
     * @param attributeName Attribute name
     * @return the related attribute value, return null if the attribute name doesn't exist.
     */
    private String getAttributeValue(final Map<String, Object> data, String attributeName) {
        if (data.containsKey(attributeName)) {
            final Object value = data.get(attributeName);
            if (value instanceof Number) {
                return new BigDecimal(value.toString()).toPlainString();
            }
            return String.valueOf(value);
        }
        return null;
    }

    void flushBuffer() throws Exception {
        bufferAccumulator.flush();
    }

    /**
     * Add event record to buffer
     *
     * @param eventCreationTimeMillis Creation timestamp of the event
     * @throws Exception Exception if failed to write to buffer.
     */
    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final JsonNode jsonNode,
                            final long eventCreationTimeMillis,
                            final long eventVersionNumber) throws Exception {
        Event event = JacksonEvent.builder()
                .withEventType(getEventType())
                .withData(jsonNode)
                .build();

        // Only set external origination time for stream events, not export
        EventMetadata eventMetadata = event.getMetadata();
        eventMetadata.setAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreationTimeMillis);
        eventMetadata.setAttribute(EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        if (acknowledgementSet != null) {
            acknowledgementSet.add(event);
        }
        bufferAccumulator.add(new Record<>(event));
    }
}
