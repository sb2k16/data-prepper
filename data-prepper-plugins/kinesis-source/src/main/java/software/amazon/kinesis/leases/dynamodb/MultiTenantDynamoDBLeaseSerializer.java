/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package software.amazon.kinesis.leases.dynamodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;
import software.amazon.kinesis.leases.DynamoUtils;
import software.amazon.kinesis.leases.MultiTenantLease;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;

public class MultiTenantDynamoDBLeaseSerializer extends DynamoDBMultiStreamLeaseSerializer {
    private static final String APP_KEY = "applicationName";
    private static final String LEASE_KEY = "leaseKey";
    private static final String STREAM_ID_KEY = "streamName";

    private final String applicationName;

    public MultiTenantDynamoDBLeaseSerializer(final String applicationName) {
        super();
        this.applicationName = applicationName;
    }

    @Override
    public Map<String, AttributeValue> toDynamoRecord(final Lease lease) {
        final Map<String, AttributeValue> result = super.toDynamoRecord(lease);
        result.put(APP_KEY, DynamoUtils.createAttributeValue(applicationName));
        return result;
    }

    @Override
    public MultiStreamLease fromDynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        final MultiStreamLease multiStreamLease = super.fromDynamoRecord(dynamoRecord);
        MultiTenantLease MultiTenantLease = new MultiTenantLease(multiStreamLease);
        MultiTenantLease.applicationName(DynamoUtils.safeGetString(dynamoRecord, APP_KEY));
        return MultiTenantLease;
    }

    @Override
    public Map<String, ExpectedAttributeValue> getDynamoNonexistantExpectation() {
        Map<String, ExpectedAttributeValue> result = super.getDynamoNonexistantExpectation();

        ExpectedAttributeValue expectedAV = ExpectedAttributeValue.builder().exists(false).build();
        result.put(APP_KEY, expectedAV);

        return result;
    }

    @Override
    public Collection<KeySchemaElement> getKeySchema() {
        Collection<KeySchemaElement> kinesisSourceKeySchema = new ArrayList<>();
        kinesisSourceKeySchema.add(KeySchemaElement.builder()
                .attributeName(APP_KEY).keyType(KeyType.HASH).build());
        kinesisSourceKeySchema.add(KeySchemaElement.builder()
                .attributeName(LEASE_KEY).keyType(KeyType.RANGE).build());

        return kinesisSourceKeySchema;
    }

    @Override
    public Collection<AttributeDefinition> getAttributeDefinitions() {
        Collection<AttributeDefinition> definitions = new ArrayList<>();
        definitions.add(AttributeDefinition.builder().attributeName(APP_KEY)
                .attributeType(ScalarAttributeType.S).build());
        definitions.add(AttributeDefinition.builder().attributeName(LEASE_KEY)
                .attributeType(ScalarAttributeType.S).build());
        definitions.add(AttributeDefinition.builder().attributeName(STREAM_ID_KEY)
                .attributeType(ScalarAttributeType.S).build());
        return definitions;
    }

    @Override
    public Map<String, AttributeValue> getDynamoHashKey(final Lease lease) {
        final MultiTenantLease multiTenantLease = MultiTenantLease.validateAndCast(lease);
        Map<String, AttributeValue> result = new HashMap<>();
        result.put(APP_KEY, DynamoUtils.createAttributeValue(multiTenantLease.applicationName()));
        result.put(LEASE_KEY, DynamoUtils.createAttributeValue(multiTenantLease.leaseKey()));
        return result;
    }

    public Collection<KeySchemaElement> getIndexSchema() {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder().attributeName(APP_KEY).keyType(KeyType.HASH).build());
        keySchema.add(KeySchemaElement.builder()
                .attributeName(STREAM_ID_KEY).keyType(KeyType.RANGE).build());

        return keySchema;
    }

}