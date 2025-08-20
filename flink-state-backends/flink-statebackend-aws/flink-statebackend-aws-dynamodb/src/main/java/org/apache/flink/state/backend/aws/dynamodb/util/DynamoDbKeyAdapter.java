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

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbAttributes;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.Base64;
import java.util.Map;

/**
 * Utility class for building DynamoDB keys for state entries.
 *
 * <p>This class helps construct the composite keys used in the DynamoDB state backend.
 * It creates a map of attribute values that can be used as the key in DynamoDB operations.
 *
 * <p>The key structure consists of:
 * <ul>
 *   <li>A partition key ({@link DynamoDbAttributes#JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR}) that combines
 *       job name, task name, state name, and state key to ensure good distribution across DynamoDB partitions</li>
 *   <li>A sort key ({@link DynamoDbAttributes#NAMESPACE_ATTR}) that contains
 *       the namespace information to enable efficient lookups</li>
 *   <li>A Global Secondary Index (GSI) with partition key {@link DynamoDbAttributes#JOB_NAME_AND_TASK_NAME_ATTR}
 *       and sort key {@link DynamoDbAttributes#STATE_DESCRIPTOR_ATTR} to enable querying all states for a job and task</li>
 * </ul>
 *
 * <p>This key structure provides several benefits:
 * <ul>
 *   <li>Efficient retrieval of specific state entries by key and namespace</li>
 *   <li>Good distribution of data across DynamoDB partitions</li>
 *   <li>Support for querying all state entries for a specific job, task, or state name</li>
 *   <li>Ability to extract key and namespace information for state iteration</li>
 * </ul>
 */
public final class DynamoDbKeyAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbKeyAdapter.class);

    /**
     * Builds the DynamoDB key for a state entry.
     *
     * <p>This method creates a map of attribute values that can be used as the key
     * in DynamoDB operations. The key consists of a partition key and a sort key.
     *
     * <p>The partition key is a combination of job name, task name, and state name,
     * formatted as: "{jobName} -> {taskName} -> {stateName}".
     *
     * <p>The sort key combines the key and namespace information,
     * formatted as: "{key} -> {namespace}".
     *
     * @param jobInfo The job information containing job ID and name
     * @param taskInfo The task information containing task name and index
     * @param stateName The name of the state (from the state descriptor)
     * @param key The key of the state entry
     * @param namespace The namespace of the state entry
     * @param <K> The type of the key
     * @param <N> The type of the namespace
     * @return A map of attribute values representing the DynamoDB key
     */
    public static <K, N> Map<String, AttributeValue> createDynamoDbKey(
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final String stateName,
            final K key,
            final N namespace) {
        LOG.trace(
                "Creating DynamoDB key for job: {}, task: {}, state: {}, key: {}, namespace: {}",
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName,
                key,
                namespace);

        // Create jobNameAndTaskName for GSI
        final String jobNameAndTaskName = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName());

        // Create jobAwareStateKeyAndDescriptor (includes state key)
        final String jobAwareStateKeyAndDescriptor = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName,
                key.toString());

        // Namespace is now the sort key directly
        final String namespaceStr = namespace.toString();

        LOG.trace("Generated partition key: {}, sort key: {}, GSI key: {}",
                jobAwareStateKeyAndDescriptor,
                namespaceStr,
                jobNameAndTaskName);

        // Using ImmutableMap.Builder since we have more than 2 entries
        final Map<String, AttributeValue> dynamoDbKey = ImmutableMap.<String, AttributeValue>builder()
                .put(DynamoDbAttributes.JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR,
                        AttributeValue.builder().s(jobAwareStateKeyAndDescriptor).build())
                .put(DynamoDbAttributes.NAMESPACE_ATTR,
                        AttributeValue.builder().s(namespaceStr).build())
                .put(DynamoDbAttributes.JOB_NAME_AND_TASK_NAME_ATTR,
                        AttributeValue.builder().s(jobNameAndTaskName).build())
                .put(DynamoDbAttributes.STATE_DESCRIPTOR_ATTR,
                        AttributeValue.builder().s(stateName).build())
                .put(DynamoDbAttributes.STATE_KEY_ATTR,
                        AttributeValue.builder().s(key.toString()).build())
                .build();

        LOG.trace("DynamoDB key created successfully");
        return dynamoDbKey;
    }

    /**
     * Extracts the serialized sort key from a DynamoDB item.
     *
     * <p>This method retrieves the sort key attribute from a DynamoDB item and
     * decodes it from Base64 format to a byte array. The sort key contains
     * the serialized key and namespace information.
     *
     * <p>This method is used during state iteration to extract the key and namespace
     * information from DynamoDB items.
     *
     * @param dynamoKey The DynamoDB item containing the sort key attribute
     * @return The decoded sort key as a byte array, or null if the sort key is not present
     */
    public static byte[] getSortKey(final Map<String, AttributeValue> dynamoKey) {
        LOG.trace("Extracting sort key from DynamoDB key");
        AttributeValue sortKeyAttr = dynamoKey.get(DynamoDbAttributes.NAMESPACE_ATTR);

        if (sortKeyAttr == null) {
            LOG.warn("Sort key attribute is null");
            return null;
        }

        // If it's a binary attribute, it's already base64 encoded bytes
        final SdkBytes sortKeyBytes = sortKeyAttr.b();
        LOG.trace("Sort key extracted from binary attribute, size: {} bytes", sortKeyBytes.asByteArray().length);
        return Base64.getDecoder().decode(sortKeyBytes.asByteArray());
    }

    /**
     * Extracts the state key from a DynamoDB item with the new schema.
     *
     * @param dynamoKey The DynamoDB item
     * @return The state key as a String, or null if not present
     */
    public static String getStateKey(final Map<String, AttributeValue> dynamoKey) {
        LOG.trace("Extracting state key from DynamoDB key");
        final AttributeValue stateKeyAttr = dynamoKey.get(DynamoDbAttributes.STATE_KEY_ATTR);

        if (stateKeyAttr == null) {
            LOG.warn("State key attribute is null");
            return null;
        }

        return stateKeyAttr.s();
    }
}
