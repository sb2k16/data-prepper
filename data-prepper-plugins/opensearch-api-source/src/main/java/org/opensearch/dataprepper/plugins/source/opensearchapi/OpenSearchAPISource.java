/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.http.LogThrottlingStrategy;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.codec.JsonDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

@DataPrepperPlugin(name = "opensearch_api", pluginType = Source.class, pluginConfigurationType = OpenSearchAPISourceConfig.class)
public class OpenSearchAPISource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAPISource.class);
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    public static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OpenSearchAPISourceConfig sourceConfig;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ArmeriaHttpAuthenticationProvider authenticationProvider;
    private final HttpRequestExceptionHandler httpRequestExceptionHandler;
    private final String pipelineName;
    private Server server;
    private final PluginMetrics pluginMetrics;
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private ByteDecoder byteDecoder;

    @DataPrepperPluginConstructor
    public OpenSearchAPISource(final OpenSearchAPISourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                      final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.byteDecoder = new JsonDecoder();
        this.certificateProviderFactory = new CertificateProviderFactory(sourceConfig);
        final PluginModel authenticationConfiguration = sourceConfig.getAuthentication();
        final PluginSetting authenticationPluginSetting;

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating OpenSearch API source without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the OpenSearch API source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#authentication-configurations");
        }

        if(authenticationConfiguration != null) {
            authenticationPluginSetting =
                    new PluginSetting(authenticationConfiguration.getPluginName(), authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting =
                    new PluginSetting(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        authenticationProvider = pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, authenticationPluginSetting);
        httpRequestExceptionHandler = new HttpRequestExceptionHandler(pluginMetrics);
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            final ServerBuilder sb = Server.builder();

            sb.disableServerHeader();

            if (sourceConfig.isSsl()) {
                LOG.info("Creating http source with SSL/TLS enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                // TODO: enable encrypted key with password
                sb.https(sourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                        )
                );
            } else {
                LOG.warn("Creating OpenSearch API source without SSL/TLS. This is not secure.");
                LOG.warn("In order to set up TLS for the OpenSearch API source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#ssl");
                sb.http(sourceConfig.getPort());
            }

            if(sourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalAuthDecorator = authenticationProvider.getAuthenticationDecorator();

                if (sourceConfig.isUnauthenticatedHealthCheck()) {
                    optionalAuthDecorator.ifPresent(authDecorator -> sb.decorator(REGEX_HEALTH, authDecorator));
                } else {
                    optionalAuthDecorator.ifPresent(sb::decorator);
                }
            }

            sb.maxNumConnections(sourceConfig.getMaxConnectionCount());
            sb.requestTimeout(Duration.ofMillis(sourceConfig.getRequestTimeoutInMillis()));
            if(sourceConfig.getMaxRequestLength() != null) {
                sb.maxRequestLength(sourceConfig.getMaxRequestLength().getBytes());
            }
            final int threads = sourceConfig.getThreadCount();
            final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threads);
            sb.blockingTaskExecutor(blockingTaskExecutor, true);
            final int maxPendingRequests = sourceConfig.getMaxPendingRequests();
            final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(
                    maxPendingRequests, blockingTaskExecutor.getQueue());
            final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests, pluginMetrics);

            final String httpSourcePath = sourceConfig.getPath().replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
            sb.decorator(httpSourcePath, ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));
            final OpenSearchAPIService openSearchAPIService = new OpenSearchAPIService(sourceConfig.getBufferTimeoutInMillis(), buffer, pluginMetrics);

            if (CompressionOption.NONE.equals(sourceConfig.getCompression())) {
                sb.annotatedService(httpSourcePath, openSearchAPIService, httpRequestExceptionHandler);
            } else {
                sb.annotatedService(httpSourcePath, openSearchAPIService, DecodingService.newDecorator(), httpRequestExceptionHandler);
            }

            if (sourceConfig.hasHealthCheckService()) {
                LOG.info("OpenSearch API source health check is enabled");
                sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
            }

            server = sb.build();
            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
        }

        try {
            server.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Started OpenSearch API source on port " + sourceConfig.getPort() + "...");
    }

    @Override
    public ByteDecoder getDecoder() {
        return byteDecoder;
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                } else {
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        LOG.info("Stopped OpenSearch API source.");
    }
}