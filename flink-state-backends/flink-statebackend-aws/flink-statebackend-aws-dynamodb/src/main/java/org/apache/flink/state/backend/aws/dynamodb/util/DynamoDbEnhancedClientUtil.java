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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;
import org.apache.flink.state.backend.aws.dynamodb.model.StateItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.IOException;

/**
 * Utility class for working with the DynamoDB Enhanced Client.
 *
 * <p>This class provides helper methods for creating and working with
 * DynamoDB Enhanced Client objects, which offer a higher-level, object-oriented
 * interface to DynamoDB compared to the standard client.
 *
 * <p>Key features:
 * <ul>
 *   <li>Creating enhanced clients from standard DynamoDB clients</li>
 *   <li>Building table mappings with appropriate schemas</li>
 *   <li>Creating partition and sort keys for state lookups</li>
 *   <li>Converting between Flink state entries and DynamoDB items</li>
 *   <li>Handling compression and serialization of state values</li>
 * </ul>
 *
 * <p>This utility is used by the DynamoDB state backend to interact with DynamoDB
 * using the enhanced client API, which simplifies many operations compared to the
 * standard client.
 */
public final class DynamoDbEnhancedClientUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbEnhancedClientUtil.class);

    /**
     * Creates a DynamoDB Enhanced Client from a standard DynamoDB client.
     *
     * @param dynamoDbClient The standard DynamoDB client
     *
     * @return A DynamoDB Enhanced Client
     */
    public static DynamoDbEnhancedClient createEnhancedClient(final DynamoDbClient dynamoDbClient) {
        LOG.debug("Creating DynamoDB Enhanced Client");
        final DynamoDbEnhancedClient client = DynamoDbEnhancedClient
                .builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
        LOG.debug("DynamoDB Enhanced Client created successfully");
        return client;
    }

    /**
     * Creates a DynamoDB table object for the StateItem class.
     *
     * @param enhancedClient The DynamoDB Enhanced Client
     * @param tableName The name of the DynamoDB table
     *
     * @return A DynamoDB table object for StateItem
     */
    public static DynamoDbTable<StateItem> createStateItemTable(
            final DynamoDbEnhancedClient enhancedClient,
            final String tableName) {
        LOG.debug("Creating DynamoDB table mapping for table: {}", tableName);
        final DynamoDbTable<StateItem> table = enhancedClient.table(
                tableName,
                TableSchema.fromBean(StateItem.class));
        LOG.debug("DynamoDB table mapping created successfully for table: {}", tableName);
        return table;
    }

    /**
     * Creates a DynamoDB key for querying state items.
     *
     * @param jobInfo The job information
     * @param taskInfo
     * @param stateName The state name
     *
     * @return A DynamoDB key
     */
    public static <K, N> Key createKey(
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final String stateName,
            final K key,
            final N namespace) {
        LOG.trace(
                "Creating DynamoDB key for job: {}, task: {}, state: {}, key: {}, namespace: {}",
                jobInfo.getJobName(), taskInfo.getTaskName(), stateName, key, namespace);

        final String keyStr = key != null ? key.toString() : "";
        final String namespaceStr = namespace != null ? namespace.toString() : "";

        // Create jobAwareStateKeyAndDescriptor with state key included
        final String partitionValueRaw = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName,
                keyStr);

        LOG.trace("Created partition key: {}, sort key: {}", partitionValueRaw, namespaceStr);

        final Key dynamoKey = Key
                .builder()
                .partitionValue(partitionValueRaw)
                .sortValue(namespaceStr)
                .build();

        LOG.trace("DynamoDB composite key created successfully");
        return dynamoKey;
    }

    /**
     * Creates a StateItem from the provided parameters.
     *
     * <p>This method builds a StateItem object that represents a state entry in DynamoDB.
     * It combines job, task, operator, and state information to create unique partition
     * and sort keys, and compresses the state value using Snappy compression to reduce
     * storage requirements.
     *
     * @param jobInfo The job information containing job ID and name
     * @param taskInfo The task information containing task name and index
     * @param operatorIdentifier The operator identifier
     * @param stateName The state name
     * @param valueBytes The serialized state value as a byte array
     * @param ttl The time-to-live value in seconds since epoch (can be null for no TTL)
     * @param key The state key
     * @param namespace The namespace
     * @return A StateItem ready to be stored in DynamoDB
     * @throws IOException If compression fails
     */
    public static <K, N> StateItem createStateItem(
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final String operatorIdentifier,
            final String stateName,
            final byte[] valueBytes,
            final Long ttl,
            final K key,
            final N namespace) throws IOException {
        LOG.debug(
                "Creating StateItem for job: {}, operator: {}, state: {}, key: {}, namespace: {}",
                jobInfo.getJobName(), operatorIdentifier, stateName, key, namespace);

        final String keyStr = key != null ? key.toString() : "";
        final String namespaceStr = namespace != null ? namespace.toString() : "";

        // Create jobAwareStateKeyAndDescriptor (includes state key)
        final String jobAwareStateKeyAndDescriptor = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName,
                keyStr);

        // Create jobNameAndTaskName for GSI
        final String jobNameAndTaskName = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName());

        LOG.trace("Compressing state value using Snappy, original size: {}", valueBytes.length);
        final byte[] valueToStore = Snappy.compress(valueBytes);
        LOG.trace(
                "Value compressed successfully, compressed size: {}, compression ratio: {}",
                valueToStore.length,
                String.format("%.2f", (double) valueToStore.length / valueBytes.length));

        LOG.trace("Building StateItem with TTL: {}", ttl);
        final StateItem item = StateItem
                .builder()
                .jobAwareStateKeyAndDescriptor(jobAwareStateKeyAndDescriptor)
                .namespace(namespaceStr)
                .jobNameAndTaskName(jobNameAndTaskName)
                .ttl(ttl)
                .stateDescriptor(stateName)
                .jobId(jobInfo.getJobId().toString())
                .jobName(jobInfo.getJobName())
                .taskName(taskInfo.getTaskName())
                .taskIndex(taskInfo.getIndexOfThisSubtask())
                .operatorId(operatorIdentifier)
                .stateKey(keyStr)
                .value(SdkBytes.fromByteArray(valueToStore))
                .build();

        LOG.debug("StateItem created successfully");
        return item;
    }

    /**
     * Converts a DynamoDB StateItem to a Flink StateEntry.
     *
     * <p>This method deserializes a StateItem retrieved from DynamoDB into a Flink StateEntry
     * that can be used by the state backend. It extracts the key, namespace, and value from
     * the StateItem and deserializes them using the provided serializers.
     *
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param stateValueSerializer The serializer for the state value
     * @param stateItem The StateItem retrieved from DynamoDB
     * @param <K> The type of the key
     * @param <N> The type of the namespace
     * @param <SV> The type of the state value
     * @return A StateEntry containing the deserialized key, namespace, and value
     * @throws RuntimeException If deserialization fails
     */

    @SuppressWarnings("unchecked")
    public static <K, N, SV> StateEntry<K, N, SV> toStateEntry(
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final TypeSerializer<SV> stateValueSerializer,
            final StateItem stateItem) {
        LOG.debug("Converting StateItem to StateEntry: {}", stateItem.getStateDescriptor());

        try {
            final byte[] valueBytes = stateItem.getValue().asByteArray();
            LOG.trace("Retrieved value bytes, size: {}", valueBytes.length);

            // Get the key and namespace directly from the item
            final String stateKey = stateItem.getStateKey();
            final String namespace = stateItem.getNamespace();

            LOG.trace("Using key: {}, namespace: {}", stateKey, namespace);

            // Deserialize the key and namespace using the appropriate serializers
            final DataInputDeserializer keyDeserializer = new DataInputDeserializer(stateKey.getBytes());
            final DataInputDeserializer namespaceDeserializer = new DataInputDeserializer(namespace.getBytes());

            final K key = keySerializer.deserialize(keyDeserializer);
            final N ns = namespaceSerializer.deserialize(namespaceDeserializer);

            LOG.trace("Deserializing state value");
            final SV deserializedValue = stateValueSerializer.deserialize(new DataInputDeserializer(
                    valueBytes));

            LOG.debug(
                    "StateEntry created successfully for key: {}, namespace: {}",
                    key,
                    ns);
            return new StateEntry.SimpleStateEntry<>(
                    keySerializer.copy(key),
                    namespaceSerializer.copy(ns),
                    deserializedValue);

        } catch (final IOException e) {
            LOG.error("Error converting StateItem to StateEntry", e);
            throw new RuntimeException("Error converting StateItem to StateEntry", e);
        }
    }

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private DynamoDbEnhancedClientUtil() {
    }
}
