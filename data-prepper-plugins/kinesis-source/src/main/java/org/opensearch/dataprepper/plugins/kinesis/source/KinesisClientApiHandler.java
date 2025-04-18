/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import software.amazon.awssdk.arns.Arn;
import com.linecorp.armeria.client.retry.Backoff;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.StreamIdentifier;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class KinesisClientApiHandler {
    private static final String COLON = ":";

    private final Backoff backoff;
    private final KinesisAsyncClient kinesisClient;
    private int failedAttemptCount;
    private int maxRetryCount;
    private final Map<String, String> streamIdentifierMap;
    private final Map<String, String> streamIdentifierToStreamArnMap;

    public KinesisClientApiHandler(final KinesisAsyncClient kinesisClient, final Backoff backoff, final int maxRetryCount) {
        this.kinesisClient = kinesisClient;
        this.backoff = backoff;
        this.failedAttemptCount = 0;
        if (maxRetryCount <= 0) {
            throw new IllegalArgumentException("Maximum Retry count should be strictly greater than zero.");
        }
        this.maxRetryCount = maxRetryCount;
        this.streamIdentifierMap = new ConcurrentHashMap<>();
        this.streamIdentifierToStreamArnMap = new ConcurrentHashMap<>();
    }

    public StreamIdentifier getStreamIdentifier(final String streamName) {
        final DescribeStreamSummaryResponse response = getStreamDescriptionSummary(streamName, null);
        String streamIdentifierString = getStreamIdentifierString(response.streamDescriptionSummary());
        this.streamIdentifierMap.put(streamIdentifierString, streamName);
        this.streamIdentifierToStreamArnMap.put(streamIdentifierString, response.streamDescriptionSummary().streamARN());
        return StreamIdentifier.multiStreamInstance(streamIdentifierString);
    }

    public StreamIdentifier getStreamIdentifierFromStreamArn(final String streamArn) {
        Arn arn = Arn.fromString(streamArn);
        String streamName = arn.resource().resource();
        final DescribeStreamSummaryResponse response = getStreamDescriptionSummary(streamName, streamArn);
        String streamIdentifierString = getStreamIdentifierString(response.streamDescriptionSummary());
        this.streamIdentifierMap.put(streamIdentifierString, streamArn);
        this.streamIdentifierToStreamArnMap.put(streamIdentifierString, response.streamDescriptionSummary().streamARN());
        return StreamIdentifier.multiStreamInstance(arn, response.streamDescriptionSummary().streamCreationTimestamp().getEpochSecond());
    }

    public Optional<String> getConsumerArnForStream(final String streamArn, final String consumerName) {
        DescribeStreamConsumerResponse response = describeStreamConsumer(streamArn, consumerName);
        if (response == null) {
            return Optional.empty();
        }
        return Optional.of(response.consumerDescription().consumerARN());
    }

    public Optional<String> getStreamInfoFromStreamIdentifier(final String streamIdentifier) {
        if (Objects.isNull(streamIdentifier) || !streamIdentifierMap.containsKey(streamIdentifier)) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.streamIdentifierMap.get(streamIdentifier));
    }

    public String getStreamArnFromStreamIdentifier(final String streamIdentifier) {
        return this.streamIdentifierToStreamArnMap.get(streamIdentifier);
    }

    private DescribeStreamSummaryResponse getStreamDescriptionSummary(final String streamName, final String streamArn) {
        DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .streamARN(streamArn)
                .build();

        for (int attempt = 0; attempt < maxRetryCount; attempt++) {
            try {
                return kinesisClient.describeStreamSummary(request).join();
            } catch (CompletionException ex) {
                handleException(ex, streamName, attempt);
            }
            applyBackoff(attempt);
        }
        throw new KinesisRetriesExhaustedException(String.format("Failed to get Kinesis stream summary for stream %s after %d retries", streamName, maxRetryCount));
    }

    private DescribeStreamConsumerResponse describeStreamConsumer(final String streamArn, final String consumerName) {
        DescribeStreamConsumerRequest request = DescribeStreamConsumerRequest.builder()
                                                .streamARN(streamArn).consumerName(consumerName).build();
        for (int attempt = 0; attempt < maxRetryCount; attempt++) {
            try {
                return kinesisClient.describeStreamConsumer(request).join();
            } catch (CompletionException ex) {
                handleException(ex, streamArn, attempt);
            }
            applyBackoff(attempt);
        }
        throw new KinesisRetriesExhaustedException(String.format("Failed to get Kinesis stream consumer for stream arn %s after %d retries", streamArn, maxRetryCount));
    }

    private void handleException(CompletionException ex, String streamName, int attempt) {
        Throwable cause = ex.getCause();
        if (cause instanceof KinesisException || cause instanceof com.amazonaws.SdkClientException) {
            log.error("Failed to describe stream summary for stream {} with error {}. Attempt {} of {}.", streamName, ex.getMessage(), attempt + 1, maxRetryCount);
        } else {
            log.error("Failed to describe stream summary for stream {} with error {}. Attempt {} of {}.", streamName, ex, attempt + 1, maxRetryCount);
        }
    }

    private void applyBackoff(int attempt) {
        final long delayMillis = backoff.nextDelayMillis(attempt);
        if (delayMillis < 0) {
            throw new KinesisRetriesExhaustedException("Kinesis DescribeStreamSummary request retries exhausted. Make sure that Kinesis configuration is valid, Kinesis stream exists, and IAM role has required permissions.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        log.info("Pausing Kinesis DescribeStreamSummary request for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
            log.error("Thread is interrupted while polling Kinesis with retry.", e);
        }
    }

    private String getStreamIdentifierString(StreamDescriptionSummary summary) {
        String accountId = Arn.fromString(summary.streamARN()).accountId().orElseThrow();
        long creationEpochSecond = summary.streamCreationTimestamp().getEpochSecond();
        return getStreamIdentifierString(accountId, summary.streamName(), creationEpochSecond);
    }

    public String getStreamIdentifierString(final String accountId, final String streamName, final long creationEpochSecond) {
        return String.join(COLON, accountId, streamName, String.valueOf(creationEpochSecond));
    }
}
