package software.amazon.kinesis.leases;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.Validate;


@Setter
@NoArgsConstructor
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
public class MultiTenantLease extends MultiStreamLease {

    @NonNull private String applicationName;
    private String pipelineName;
    private String accountId;

    public MultiTenantLease(MultiTenantLease other) {
        super(other);
        applicationName(other.applicationName);
        pipelineName(other.pipelineName);
        accountId(other.accountId);
    }

    public MultiTenantLease(MultiStreamLease multiStreamLease) {
        super(multiStreamLease);
    }

    @Override
    public void update(Lease other) {
        MultiTenantLease casted = validateAndCast(other);
        super.update(casted);
        applicationName(casted.applicationName);
        pipelineName(casted.pipelineName);
        accountId(casted.accountId);
    }

    /**
     * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
     *
     * @return A deep copy of this object.
     */
    @Override
    public MultiTenantLease copy() {
        return new MultiTenantLease(this);
    }

    /**
     * Validate and cast the lease to MultiStream lease
     * @param lease
     * @return MultiStreamLease
     */
    public static MultiTenantLease validateAndCast(Lease lease) {
        Validate.isInstanceOf(MultiTenantLease.class, lease);
        return (MultiTenantLease) lease;
    }

}
