package software.amazon.kinesis.leases.dynamodb;

import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.FutureUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.retrieval.AWSExceptionManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MultiTenantDynamoDBLeaseRefresher extends DynamoDBLeaseRefresher {
    private static final Logger log = LoggerFactory.getLogger(MultiTenantDynamoDBLeaseRefresher.class);

    private final String applicationName;
    private final boolean leaseTableDeletionProtectionEnabled;
    private final BillingMode billingMode;
    private final Collection<Tag> tags;
    private final Duration dynamoDbRequestTimeout;
    private boolean newTableCreated = false;
    private static final String DDB_STREAM_NAME = ":streamName";
    private static final String DDB_APP_NAME = ":applicationName";
    private static final String INDEX_NAME = "LeaseCoordinationIndex";

    public MultiTenantDynamoDBLeaseRefresher(final String applicationName, final String tableName, final DynamoDbAsyncClient dynamoDBClient,
                                         final LeaseSerializer serializer, final boolean consistentReads,
                                         @NonNull final TableCreatorCallback tableCreatorCallback, Duration dynamoDbRequestTimeout,
                                         final BillingMode billingMode, final boolean leaseTableDeletionProtectionEnabled,
                                         final Collection<Tag> tags) {
        super(tableName, dynamoDBClient, serializer, consistentReads, tableCreatorCallback, dynamoDbRequestTimeout,
                billingMode, leaseTableDeletionProtectionEnabled, tags);
        this.applicationName = applicationName;
        this.leaseTableDeletionProtectionEnabled = leaseTableDeletionProtectionEnabled;
        this.dynamoDbRequestTimeout = dynamoDbRequestTimeout;
        this.billingMode = billingMode;
        this.tags = tags;
    }

    @Override
    public boolean createLeaseTableIfNotExists()
            throws ProvisionedThroughputException, DependencyException {
        final CreateTableRequest request = createTableRequestBuilder().build();

        return createTableIfNotExists(request);
    }

    private boolean createTableIfNotExists(CreateTableRequest request)
            throws ProvisionedThroughputException, DependencyException {
        try {
            if (tableStatus() != null) {
                return newTableCreated;
            }
        } catch (DependencyException de) {
            //
            // Something went wrong with DynamoDB
            //
            log.error("Failed to get table status for {}", table, de);
        }

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceInUseException.class, t -> t);
        exceptionManager.add(LimitExceededException.class, t -> t);

        try {
            try {
                FutureUtils.resolveOrCancelFuture(dynamoDBClient.createTable(request), dynamoDbRequestTimeout);
                newTableCreated = true;
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new DependencyException(e);
            }
        } catch (ResourceInUseException e) {
            log.info("Table {} already exists.", table);
            return newTableCreated;
        } catch (LimitExceededException e) {
            throw new ProvisionedThroughputException("Capacity exceeded when creating table " + table, e);
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }
        return newTableCreated;
    }

    private AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(DynamoDbException.class, t -> t);
        return exceptionManager;
    }

    private TableStatus tableStatus() throws DependencyException {
        DescribeTableRequest request = DescribeTableRequest.builder().tableName(table).build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);

        DescribeTableResponse result;
        try {
            try {
                result = FutureUtils.resolveOrCancelFuture(dynamoDBClient.describeTable(request), dynamoDbRequestTimeout);
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (ResourceNotFoundException e) {
            log.debug("Got ResourceNotFoundException for table {} in leaseTableExists, returning false.", table);
            return null;
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }

        TableStatus tableStatus = result.table().tableStatus();
        log.debug("Lease table exists and is in status {}", tableStatus);

        return tableStatus;
    }

    private CreateTableRequest.Builder createTableRequestBuilder() {
        GlobalSecondaryIndex leaseCoordinationIndex = GlobalSecondaryIndex.builder()
                .indexName(INDEX_NAME)
                .keySchema(serializer.getIndexSchema())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build();

        final CreateTableRequest.Builder builder = CreateTableRequest.builder()
                .tableName(table)
                .keySchema(serializer.getKeySchema())
                .attributeDefinitions(serializer.getAttributeDefinitions())
                .globalSecondaryIndexes(leaseCoordinationIndex)
                .deletionProtectionEnabled(leaseTableDeletionProtectionEnabled)
                .tags(tags);
        if (BillingMode.PAY_PER_REQUEST.equals(billingMode)) {
            builder.billingMode(billingMode);
        }
        return builder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier) throws DependencyException,
            InvalidStateException, ProvisionedThroughputException {
        return list( null, streamIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Lease> listLeases() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLeaseTableEmpty()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(1, 1, null).isEmpty();
    }

    /**
     * List with the given page size. Package access for integration testing.
     *
     * @param limit number of items to consider at a time - used by integration tests to force paging.
     * @param streamIdentifier streamIdentifier for multi-stream mode. Can be null.
     * @return list of leases
     * @throws InvalidStateException if table does not exist
     * @throws DependencyException if DynamoDB scan fail in an unexpected way
     * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
     */
    List<Lease> list(Integer limit, StreamIdentifier streamIdentifier)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        return list(limit, Integer.MAX_VALUE, streamIdentifier);
    }

    /**
     * List with the given page size. Package access for integration testing.
     *
     * @param limit number of items to consider at a time - used by integration tests to force paging.
     * @param maxPages mad paginated scan calls
     * @param streamIdentifier streamIdentifier for multi-stream mode. Can be null.
     * @return list of leases
     * @throws InvalidStateException if table does not exist
     * @throws DependencyException if DynamoDB scan fail in an unexpected way
     * @throws ProvisionedThroughputException if DynamoDB scan fail due to exceeded capacity
     */
    private List<Lease> list(Integer limit, Integer maxPages, StreamIdentifier streamIdentifier) throws DependencyException, InvalidStateException,
            ProvisionedThroughputException {

        log.debug("Listing leases from table {}", table);

        QueryRequest.Builder queryRequestBuilder = QueryRequest.builder().tableName(table);

        if (streamIdentifier != null) {
            final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                    DDB_STREAM_NAME, AttributeValue.builder().s(streamIdentifier.serialize()).build(),
                    DDB_APP_NAME, AttributeValue.builder().s(applicationName).build()
            );
            queryRequestBuilder
                    .indexName(INDEX_NAME)
                    .keyConditionExpression("streamName=:streamName and applicationName=:applicationName")
                    .expressionAttributeValues(expressionAttributeValues)
                    .consistentRead(consistentReads);
        } else {
            final Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(
                    DDB_APP_NAME, AttributeValue.builder().s(applicationName).build()
            );
            queryRequestBuilder.keyConditionExpression("applicationName=:applicationName")
                    .expressionAttributeValues(expressionAttributeValues)
                    .consistentRead(consistentReads);
        }

        if (limit != null) {
            queryRequestBuilder = queryRequestBuilder.limit(limit);
        }
        QueryRequest queryRequest = queryRequestBuilder.build();

        final AWSExceptionManager exceptionManager = createExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t ->  t);
        exceptionManager.add(ProvisionedThroughputExceededException.class, t -> t);

        try {
            try {
                QueryResponse queryResponse = FutureUtils.resolveOrCancelFuture(dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout);
                List<Lease> result = new ArrayList<>();

                while (queryResponse != null) {
                    for (Map<String, AttributeValue> item : queryResponse.items()) {
                        log.debug("Got item {} from DynamoDB.", item.toString());
                        result.add(serializer.fromDynamoRecord(item));
                    }

                    Map<String, AttributeValue> lastEvaluatedKey = queryResponse.lastEvaluatedKey();
                    if (CollectionUtils.isNullOrEmpty(lastEvaluatedKey) || --maxPages <= 0) {
                        // Signify that we're done.
                        queryResponse = null;
                        log.debug("lastEvaluatedKey was null - scan finished.");
                    } else {
                        // Make another request, picking up where we left off.
                        queryRequest = queryRequest.toBuilder().exclusiveStartKey(lastEvaluatedKey).build();
                        log.debug("lastEvaluatedKey was {}, continuing scan.", lastEvaluatedKey);
                        queryResponse = FutureUtils.resolveOrCancelFuture(dynamoDBClient.query(queryRequest), dynamoDbRequestTimeout);
                    }
                }
                log.debug("Listed {} leases from table {}", result.size(), table);
                return result;
            } catch (ExecutionException e) {
                throw exceptionManager.apply(e.getCause());
            } catch (InterruptedException e) {
                // TODO: Check if this is the correct behavior
                throw new DependencyException(e);
            }
        } catch (ResourceNotFoundException e) {
            throw new InvalidStateException("Cannot scan lease table " + table + " because it does not exist.", e);
        } catch (ProvisionedThroughputExceededException e) {
            throw new ProvisionedThroughputException(e);
        } catch (DynamoDbException | TimeoutException e) {
            throw new DependencyException(e);
        }
    }
}