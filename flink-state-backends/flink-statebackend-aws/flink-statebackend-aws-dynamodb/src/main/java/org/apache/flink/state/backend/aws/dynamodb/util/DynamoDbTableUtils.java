/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.dynamodb.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbAttributes;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class for DynamoDB table operations.
 *
 * <p>This class provides methods for creating and managing DynamoDB tables used for state storage.
 * It handles table creation with appropriate schema and capacity settings based on configuration.
 */
public final class DynamoDbTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbTableUtils.class);

    private static final AtomicBoolean IS_FIRST_REQUEST = new AtomicBoolean(true);
    /**
     * Gets the DynamoDB table for state storage or creates it if it doesn't exist.
     *
     * <p>This method checks if the configured DynamoDB table exists and creates i
     * with the appropriate schema if it doesn't. The table is configured according
     * to the capacity settings in the configuration.
     *
     * <p>The table schema includes the following attributes:
     * <ul>
     *   <li>Partition key: {@link DynamoDbAttributes#JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR}</li>
     *   <li>Sort key: {@link DynamoDbAttributes#NAMESPACE_ATTR}</li>
     *   <li>Global Secondary Index with partition key {@link DynamoDbAttributes#JOB_NAME_AND_TASK_NAME_ATTR}
     *       and sort key {@link DynamoDbAttributes#STATE_DESCRIPTOR_ATTR}</li>
     *   <li>Additional attributes for state metadata and values</li>
     * </ul>
     *
     * @return The DynamoDB table description.
     *
     * @throws RuntimeException If an error occurs while creating or accessing the table.
     */
    public static TableDescription getOrCreateTable(
            final DynamoDbClient dynamoDbClient,
            final Configuration configuration) {
        final String tableName = Preconditions.checkNotNull(
                configuration.get(DynamoDbStateBackendOptions.TABLE_NAME),
                "Table name must be specified");
        final DescribeTableRequest describeTableRequest = DescribeTableRequest
                .builder()
                .tableName(tableName)
                .build();

        // Create the table on the first request, if necessary
        if (IS_FIRST_REQUEST.getAndSet(false)) {
            try {
                return dynamoDbClient.describeTable(describeTableRequest).table();
            } catch (final ResourceNotFoundException e) {
                createTable(dynamoDbClient, configuration, tableName);
            }
        }

        return dynamoDbClient
                .waiter()
                .waitUntilTableExists(describeTableRequest)
                .matched()
                .response()
                .map(DescribeTableResponse::table)
                .orElseThrow(RuntimeException::new);
    }

    private static void createTable(
            final DynamoDbClient dynamoDbClient,
            final Configuration configuration,
            final String tableName) {
        // Table does not exist, create it
        LOG.info("Creating DynamoDB table: {}", tableName);

        final long readCapacityUnits = configuration.get(DynamoDbStateBackendOptions.READ_CAPACITY_UNITS);
        final long writeCapacityUnits = configuration.get(DynamoDbStateBackendOptions.WRITE_CAPACITY_UNITS);
        final boolean onDemandMode = configuration.get(DynamoDbStateBackendOptions.USE_ON_DEMAND);

        final CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest
                .builder()
                .tableName(tableName)
                .keySchema(
                        KeySchemaElement
                                .builder()
                                .attributeName(DynamoDbAttributes.JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR)
                                .keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement
                                .builder()
                                .attributeName(DynamoDbAttributes.NAMESPACE_ATTR)
                                .keyType(KeyType.RANGE)
                                .build())
                .attributeDefinitions(
                        AttributeDefinition
                                .builder()
                                .attributeName(DynamoDbAttributes.JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition
                                .builder()
                                .attributeName(DynamoDbAttributes.NAMESPACE_ATTR)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition
                                .builder()
                                .attributeName(DynamoDbAttributes.JOB_NAME_AND_TASK_NAME_ATTR)
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                        AttributeDefinition
                                .builder()
                                .attributeName(DynamoDbAttributes.STATE_DESCRIPTOR_ATTR)
                                .attributeType(ScalarAttributeType.S)
                                .build())
                .globalSecondaryIndexes(
                        GlobalSecondaryIndex
                                .builder()
                                .indexName(DynamoDbAttributes.GSI_JOB_NAME_TASK_NAME_INDEX)
                                .keySchema(
                                        KeySchemaElement
                                                .builder()
                                                .attributeName(DynamoDbAttributes.JOB_NAME_AND_TASK_NAME_ATTR)
                                                .keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement
                                                .builder()
                                                .attributeName(DynamoDbAttributes.STATE_DESCRIPTOR_ATTR)
                                                .keyType(KeyType.RANGE)
                                                .build())
                                .projection(
                                        Projection
                                                .builder()
                                                .projectionType(ProjectionType.INCLUDE)
                                                .nonKeyAttributes(
                                                        DynamoDbAttributes.STATE_KEY_ATTR,
                                                        DynamoDbAttributes.NAMESPACE_ATTR)
                                                .build())
                                .provisionedThroughput(
                                        onDemandMode
                                                ? null
                                                : ProvisionedThroughput
                                                        .builder()
                                                        .readCapacityUnits(readCapacityUnits)
                                                        .writeCapacityUnits(writeCapacityUnits)
                                                        .build())
                                .build())
                .deletionProtectionEnabled(true);

        if (onDemandMode) {
            createTableRequestBuilder.billingMode(BillingMode.PAY_PER_REQUEST);
        } else {
            createTableRequestBuilder.provisionedThroughput(ProvisionedThroughput
                    .builder()
                    .readCapacityUnits(readCapacityUnits)
                    .writeCapacityUnits(writeCapacityUnits)
                    .build());
        }

        dynamoDbClient.createTable(createTableRequestBuilder.build());
    }
}
