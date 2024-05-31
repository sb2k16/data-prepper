package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpenSearchAPISourceConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "opensearch_api";

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {
        // Prepare
        final OpenSearchAPISourceConfig sourceConfig = new OpenSearchAPISourceConfig();

        // When/Then
        assertEquals(OpenSearchAPISourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(OpenSearchAPISourceConfig.DEFAULT_ENDPOINT_URI, sourceConfig.getPath());
        assertEquals(CompressionOption.NONE, sourceConfig.getCompression());
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OpenSearchAPISourceConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OpenSearchAPISourceConfig openSearchAPISourceConfig = OBJECT_MAPPER.convertValue(
                pluginSetting.getSettings(), OpenSearchAPISourceConfig.class);

        // When/Then
        assertEquals(compressionOption, openSearchAPISourceConfig.getCompression());
    }

    @Test
    void getPath_should_return_correct_path() throws NoSuchFieldException, IllegalAccessException {
        final OpenSearchAPISourceConfig objectUnderTest = new OpenSearchAPISourceConfig();

        reflectivelySetField(objectUnderTest, "path", "/my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(true));
        assertThat(objectUnderTest.getPath(), equalTo("/my/custom/path"));
    }

    @Test
    void isPathValid_should_return_false_for_invalid_path() throws NoSuchFieldException, IllegalAccessException {
        final OpenSearchAPISourceConfig objectUnderTest = new OpenSearchAPISourceConfig();

        reflectivelySetField(objectUnderTest, "path", "my/custom/path");

        assertThat(objectUnderTest.isPathValid(), equalTo(false));
    }

    @Test
    void get_s3_should_return_correct_config() throws NoSuchFieldException, IllegalAccessException {
        final OpenSearchAPISourceConfig objectUnderTest = new OpenSearchAPISourceConfig();

        reflectivelySetField(objectUnderTest, "s3Bucket", "my-bucket");
        reflectivelySetField(objectUnderTest, "s3Prefix", "my-prefix");
        reflectivelySetField(objectUnderTest, "s3Region", "my-region");

        assertThat(objectUnderTest.getS3Bucket(), equalTo("my-bucket"));
        assertThat(objectUnderTest.getS3Prefix(), equalTo("my-prefix"));
        assertThat(objectUnderTest.getS3Region(), equalTo("my-region"));
    }

    @Test
    void get_acknowledgements_should_return_correct_acknowledgements() throws NoSuchFieldException, IllegalAccessException {
        final OpenSearchAPISourceConfig objectUnderTest = new OpenSearchAPISourceConfig();

        reflectivelySetField(objectUnderTest, "acknowledgments", true);

        assertEquals(true, objectUnderTest.getAcknowledgments());
    }

    @Test
    void get_deleteS3ObjectsOnRead_should_return_correct_deleteS3ObjectsOnRead() throws NoSuchFieldException, IllegalAccessException {
        final OpenSearchAPISourceConfig objectUnderTest = new OpenSearchAPISourceConfig();

        reflectivelySetField(objectUnderTest, "deleteS3ObjectsOnRead", true);

        assertEquals(true, objectUnderTest.isDeleteS3ObjectsOnRead());
    }
    private void reflectivelySetField(final OpenSearchAPISourceConfig openSearchAPISourceConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = OpenSearchAPISourceConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(openSearchAPISourceConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
