/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.kinesis.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.kinesis.coordination.partition.StreamPartition;


import java.util.function.Function;

/**
 * Special partition factory just for this DynamoDB source.
 */
public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {


    @Override
    public EnhancedSourcePartition apply(SourcePartitionStoreItem partitionStoreItem) {
        String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        if (StreamPartition.PARTITION_TYPE.equals(partitionType)) {
            return new StreamPartition(partitionStoreItem);
        } else if (LeaderPartition.PARTITION_TYPE.equals(partitionType)) {
            return new LeaderPartition(partitionStoreItem);
        } else {
            // Unable to acquire other partitions.
            return new GlobalState(partitionStoreItem);
        }
    }


}
