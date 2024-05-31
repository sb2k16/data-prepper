/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.http.HttpServerConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

public class OpenSearchAPISourceConfig extends HttpServerConfig {
    static final String DEFAULT_ENDPOINT_URI = "/opensearch";
    static final int DEFAULT_PORT = 9202;
    static final String COMPRESSION = "compression";

    @Getter
    @JsonProperty("port")
    @Min(0)
    @Max(65535)
    private int port = DEFAULT_PORT;

    @Getter
    @JsonProperty("path")
    @Size(min = 1, message = "path length should be at least 1")
    private String path = DEFAULT_ENDPOINT_URI;

    @AssertTrue(message = "path should start with /")
    boolean isPathValid() {
        return path.startsWith("/");
    }

    @Getter
    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;

    @Getter
    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @Getter
    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @Getter
    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("aws")
    //@NotNull
    @Valid
    private AwsAuthenticationOptions awsConfig;

    @Getter
    @JsonProperty("delete_s3_objects_on_read")
    private boolean deleteS3ObjectsOnRead = false;

    @Getter
    @JsonProperty(COMPRESSION)
    private CompressionOption compression = CompressionOption.NONE;
}
