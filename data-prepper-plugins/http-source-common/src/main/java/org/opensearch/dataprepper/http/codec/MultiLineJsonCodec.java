package org.opensearch.dataprepper.http.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MultiLineJsonCodec implements Codec<List<Map<String, Object>>>  {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String REGEX = "\\r?\\n";
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE =
            new TypeReference<Map<String, Object>>() {};

    @Override
    public List<Map<String, Object>> parse(HttpData httpData) throws IOException {
        final InputStream inputStream = httpData.toInputStream();
        Objects.requireNonNull(inputStream, "InputStream cannot be null");

        String requestBody = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        List<String> jsonLines = Arrays.asList(requestBody.split(REGEX));
        if (jsonLines.isEmpty()) {
            throw new IOException("Bad request data.");
        }

        if (jsonLines.stream().allMatch(MultiLineJsonCodec::isEmpty)) {
            throw new IOException("Bad request data!");
        }

        try {
            return jsonLines.stream()
                    .map(MultiLineJsonCodec::jsonStringToMap)
                    .collect(Collectors.toList());
        } catch (UncheckedIOException ex) {
            throw new IOException(ex);
        }
    }

    private static boolean isEmpty(final String str) {
        return str.isEmpty() || str.isBlank();
    }

    private static Map<String, Object> jsonStringToMap(final String jsonString) {
        try {
            return objectMapper.readValue(jsonString, MAP_TYPE_REFERENCE);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

}
