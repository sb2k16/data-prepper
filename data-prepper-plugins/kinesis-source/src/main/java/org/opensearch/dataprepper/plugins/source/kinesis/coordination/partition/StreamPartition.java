/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.kinesis.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.state.StreamProgressState;

import java.util.Optional;

public class StreamPartition extends EnhancedSourcePartition<StreamProgressState> {

    public static final String PARTITION_TYPE = "STREAM";

    private final String streamArn;

    private final String shardId;

    private final StreamProgressState state;

    public StreamPartition(String streamArn, String shardId, Optional<StreamProgressState> state) {
        this.streamArn = streamArn;
        this.shardId = shardId;
        this.state = state.orElse(null);
    }

    public StreamPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        streamArn = keySplits[0];
        shardId = keySplits[1];
        this.state = convertStringToPartitionProgressState(StreamProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return streamArn + "|" + shardId;
    }

    @Override
    public Optional<StreamProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public String getStreamArn() {
        return streamArn;
    }

    public String getShardId() {
        return shardId;
    }
}
