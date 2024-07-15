package software.amazon.kinesis.leases;

import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.awssdk.utils.CollectionUtils;

import static software.amazon.kinesis.common.HashKeyRangeForLease.fromHashKeyRange;

public class MultiTenantHierarchicalShardSyncer  extends HierarchicalShardSyncer {
    public synchronized Lease createLeaseForChildShard(final ChildShard childShard, final StreamIdentifier streamIdentifier) throws InvalidStateException {
        /*
         * LeaseKey used by Extended KCL is streamId@shardId, which is normally overridden through
         * KinesisMultiStreamClientFacade which morphs ShardId in KCL responses to match this format.
         * However, when a child shard is created by split/merge ChildShard POJO is not morphed which results in this
         * code path into using incorrect leaseKey. Overriding the method to provide the correct leaseKey.
         */
        return newKCLMultiTenantLeaseForChildShard(childShard, streamIdentifier);
    }

    private static Lease newKCLMultiTenantLeaseForChildShard(final ChildShard childShard,
                                                             final StreamIdentifier streamIdentifier) throws InvalidStateException {
        MultiTenantLease newLease = new MultiTenantLease();
        newLease.leaseKey(MultiTenantLease.getLeaseKey(streamIdentifier.serialize(), childShard.shardId()));
        if (!CollectionUtils.isNullOrEmpty(childShard.parentShards())) {
            newLease.parentShardIds(childShard.parentShards());
        } else {
            throw new InvalidStateException("Unable to populate new lease for child shard " + childShard.shardId()
                    + " because parent shards cannot be found.");
        }
        newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
        newLease.ownerSwitchesSinceCheckpoint(0L);
        newLease.streamIdentifier(streamIdentifier.serialize());
        newLease.shardId(childShard.shardId());
        newLease.hashKeyRange(fromHashKeyRange(childShard.hashKeyRange()));
        return newLease;
    }
}

