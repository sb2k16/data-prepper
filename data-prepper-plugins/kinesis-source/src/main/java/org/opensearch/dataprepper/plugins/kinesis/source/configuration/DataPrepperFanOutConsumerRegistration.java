package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.kinesis.common.KinesisRequestsBuilder;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.ConsumerRegistration;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Accessors(fluent = true)
public class DataPrepperFanOutConsumerRegistration implements ConsumerRegistration {
    @NonNull
    private final KinesisAsyncClient kinesisClient;
    private final String streamName;
    @NonNull
    private final String streamConsumerName;
    private final int maxDescribeStreamSummaryRetries;
    private final int maxDescribeStreamConsumerRetries;
    private final int registerStreamConsumerRetries;
    private final long retryBackoffMillis;
    private final long STREAM_CONSUMER_TIMEOUT_IN_MIN = 2;
    private String streamArn;
    @Setter(AccessLevel.PRIVATE)
    private String streamConsumerArn;

    public DataPrepperFanOutConsumerRegistration(@NonNull KinesisAsyncClient kinesisClient, String streamName, @NonNull String streamConsumerName, int maxDescribeStreamSummaryRetries, int maxDescribeStreamConsumerRetries, int registerStreamConsumerRetries, long retryBackoffMillis, String streamArn) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.streamConsumerName = streamConsumerName;
        this.maxDescribeStreamSummaryRetries = maxDescribeStreamSummaryRetries;
        this.maxDescribeStreamConsumerRetries = maxDescribeStreamConsumerRetries;
        this.registerStreamConsumerRetries = registerStreamConsumerRetries;
        this.retryBackoffMillis = retryBackoffMillis;
        this.streamArn = streamArn;
    }

    /**
     * @inheritDoc
     */
    @Override
    public String getOrCreateStreamConsumerArn() throws DependencyException {
        if (StringUtils.isEmpty(streamConsumerArn)) {
            DescribeStreamConsumerResponse response = null;

            // 1. Check if consumer exists
            try {
                response = describeStreamConsumer();
            } catch (ResourceNotFoundException e) {
                log.info("StreamConsumer not found, need to create it.");
            }

            // 2. If not, register consumer
            if (response == null) {
                LimitExceededException finalException = null;
                int retries = registerStreamConsumerRetries;
                try {
                    while (retries > 0) {
                        finalException = null;
                        try {
                            final RegisterStreamConsumerResponse registerResponse = registerStreamConsumer();
                            streamConsumerArn(registerResponse.consumer().consumerARN());
                            break;
                        } catch (LimitExceededException e) {
                            // TODO: Figure out internal service exceptions
                            log.debug("RegisterStreamConsumer call got throttled will retry.");
                            finalException = e;
                        }
                        retries--;
                    }

                    // All calls got throttled, returning.
                    if (finalException != null) {
                        throw new DependencyException(finalException);
                    }
                } catch (ResourceInUseException e) {
                    // Consumer is present, call DescribeStreamConsumer
                    log.debug("Got ResourceInUseException consumer exists, will call DescribeStreamConsumer again.");
                    response = describeStreamConsumer();
                }
            }

            // Update consumer arn, if describe was successful.
            if (response != null) {
                streamConsumerArn(response.consumerDescription().consumerARN());
            }

            // Check if consumer is active before proceeding
            waitForActive();
        }
        return streamConsumerArn;
    }

    private RegisterStreamConsumerResponse registerStreamConsumer() throws DependencyException {
        final AWSExceptionManager exceptionManager = createExceptionManager();
        try {
            final RegisterStreamConsumerRequest request = KinesisRequestsBuilder
                    .registerStreamConsumerRequestBuilder().streamARN(streamArn())
                    .consumerName(streamConsumerName).build();
            CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumerResponseCompletableFuture = kinesisClient.registerStreamConsumer(request);
            try {
                return registerStreamConsumerResponseCompletableFuture.get(STREAM_CONSUMER_TIMEOUT_IN_MIN, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                registerStreamConsumerResponseCompletableFuture.cancel(true);
                throw exceptionManager.apply(e);
            }
        } catch (ExecutionException e) {
            throw exceptionManager.apply(e.getCause());
        } catch (InterruptedException e) {
            throw new DependencyException(e);
        }
    }

    private DescribeStreamConsumerResponse describeStreamConsumer() throws DependencyException {
        final DescribeStreamConsumerRequest.Builder requestBuilder = KinesisRequestsBuilder
                .describeStreamConsumerRequestBuilder();
        final DescribeStreamConsumerRequest request;

        if (StringUtils.isEmpty(streamConsumerArn)) {
            request = requestBuilder.streamARN(streamArn()).consumerName(streamConsumerName).build();
        } else {
            request = requestBuilder.consumerARN(streamConsumerArn).build();
        }

        return retryWhenThrottled(request, maxDescribeStreamConsumerRetries, "DescribeStreamConsumer");
    }

    private void waitForActive() throws DependencyException {
        ConsumerStatus status = null;

        int retries = maxDescribeStreamConsumerRetries;

        while (!ConsumerStatus.ACTIVE.equals(status) && retries > 0) {
            status = describeStreamConsumer().consumerDescription().consumerStatus();
            retries--;
        }

        if (!ConsumerStatus.ACTIVE.equals(status)) {
            final String message = String.format(
                    "Status of StreamConsumer %s, was not ACTIVE after all retries. Was instead %s.",
                    streamConsumerName, status);
            log.error(message);
            throw new IllegalStateException(message);
        }
    }

    private String streamArn() throws RuntimeException {
        return streamArn;
    }

    private DescribeStreamConsumerResponse retryWhenThrottled(@NonNull DescribeStreamConsumerRequest request, final int maxRetries,
                                                              @NonNull final String apiName) throws DependencyException {
        final AWSExceptionManager exceptionManager = createExceptionManager();

        LimitExceededException finalException = null;

        int retries = maxRetries;
        while (retries > 0) {
            try {
                CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumerResponseCompletableFuture = kinesisClient.describeStreamConsumer(request);
                try {
                    return describeStreamConsumerResponseCompletableFuture.get(STREAM_CONSUMER_TIMEOUT_IN_MIN, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    describeStreamConsumerResponseCompletableFuture.cancel(true);
                    throw exceptionManager.apply(e);
                } catch (ExecutionException e) {
                    throw exceptionManager.apply(e.getCause());
                } catch (InterruptedException e) {
                    throw new DependencyException(e);
                }
            } catch (LimitExceededException e) {
                log.info("Throttled while calling {} API, will backoff.", apiName);
                try {
                    Thread.sleep(retryBackoffMillis + (long) (Math.random() * 100));
                } catch (InterruptedException ie) {
                    log.debug("Sleep interrupted, shutdown invoked.");
                }
                finalException = e;
            }
            retries--;
        }

        if (finalException == null) {
            throw new IllegalStateException(
                    String.format("Finished all retries and no exception was caught while calling %s", apiName));
        }

        throw finalException;
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(LimitExceededException.class, t -> t);
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);

        return exceptionManager;
    }
}