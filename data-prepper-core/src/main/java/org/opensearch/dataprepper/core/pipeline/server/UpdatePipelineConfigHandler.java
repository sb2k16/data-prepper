package org.opensearch.dataprepper.core.pipeline.server;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.opensearch.dataprepper.core.DataPrepper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;

/**
 * HttpHandler to handle requests for listing pipelines running on the data prepper instance
 */
public class UpdatePipelineConfigHandler implements HttpHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Logger LOG = LoggerFactory.getLogger(ListPipelinesHandler.class);
    private final DataPrepper dataPrepper;

    public UpdatePipelineConfigHandler(final DataPrepper dataPrepper) {
        this.dataPrepper = dataPrepper;
    }


    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.POST)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
        try {
            InputStream body = exchange.getRequestBody();
            dataPrepper.setNewConfig(body);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        } catch (Exception e) {
            LOG.error("FAILED");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        } finally {
            exchange.getResponseBody().close();
        }
    }
}