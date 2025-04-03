/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KinesisStreamConfigTest {

    private final String streamConfigJsonString = "{\n\t\"stream_name\": \"stream-1\",\n\t\"initial_position\": \"LATEST\",\n\t\"checkpoint_interval\": \"20s\"\n}";
    private final String streamArnConfigJsonString = "{\n\t\"stream_arn\": \"arn:aws:kinesis::123456789012:stream/stream-1\",\n\t\"initial_position\": \"LATEST\",\n\t\"checkpoint_interval\": \"20s\"\n}";
    private final String streamArnConfigInvalidJsonString = "{\n\t\"stream_name\": \"stream-1\",\n\t\"stream_arn\": \"arn:aws:kinesis::123456789012:stream/stream-1\",\n\t\"initial_position\": \"LATEST\",\n\t\"checkpoint_interval\": \"20s\"\n}";

    private static ObjectMapper objectMapper;
    private static Validator validator;

    @BeforeAll
    public static void setup() {
        objectMapper = new ObjectMapper();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.registerModule(simpleModule);

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    public void testValidConfigurationStreamName() throws IOException {
        KinesisStreamConfig kinesisStreamConfig = objectMapper.readValue(streamConfigJsonString.getBytes(), KinesisStreamConfig.class);
        assertEquals(kinesisStreamConfig.getName(), "stream-1");
        assertNull(kinesisStreamConfig.getArn());
        assertTrue(validator.validate(kinesisStreamConfig).isEmpty());
    }

    @Test
    public void testValidConfigurationStreamArn() throws IOException {
        KinesisStreamConfig kinesisStreamConfig = objectMapper.readValue(streamArnConfigJsonString.getBytes(), KinesisStreamConfig.class);
        assertEquals(kinesisStreamConfig.getArn(), "arn:aws:kinesis::123456789012:stream/stream-1");
        assertNull(kinesisStreamConfig.getName());
        assertTrue(validator.validate(kinesisStreamConfig).isEmpty());
    }

    @Test
    public void testInValidConfigurationStreamArn() throws IOException {
        KinesisStreamConfig kinesisStreamConfig = objectMapper.readValue(streamArnConfigInvalidJsonString.getBytes(), KinesisStreamConfig.class);
        assertFalse(validator.validate(kinesisStreamConfig).isEmpty());
    }
}
