/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.dynamodb;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbAttributes;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;
import org.apache.flink.state.backend.aws.dynamodb.metrics.DynamoDbStateBackendMetrics;
import org.apache.flink.state.backend.aws.dynamodb.util.DynamoDbKeyAdapter;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.state.backend.aws.state.AwsStateIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link org.apache.flink.runtime.state.KeyedStateBackend} that stores its state in AWS DynamoDB.
 *
 * <p>This state backend implementation uses Amazon DynamoDB as the persistent storage for
 * keyed state in Flink applications. It supports all standard Flink state types (ValueState,
 * ListState, MapState, ReducingState, AggregatingState) and provides exactly-once semantics
 * through the use of checkpoints and savepoints.
 *
 * <p>Key features:
 * <ul>
 *   <li>Durable state storage in DynamoDB with automatic table management</li>
 *   <li>Support for all standard Flink state types</li>
 *   <li>Automatic compression of state data using Snappy</li>
 *   <li>TTL (Time-To-Live) support with configurable expiration policies</li>
 *   <li>Comprehensive metadata storage for debugging and monitoring</li>
 *   <li>Incremental checkpointing support</li>
 * </ul>
 *
 * <p>The state is stored in a DynamoDB table with a composite primary key consisting of:
 * <ul>
 *   <li>Partition key: A combination of job name, task name, state name, and state key</li>
 *   <li>Sort key: The namespace</li>
 * </ul>
 *
 * <p>A Global Secondary Index allows efficient querying by job and task:
 * <ul>
 *   <li>GSI Partition key: A combination of job name and task name</li>
 *   <li>GSI Sort key: State descriptor</li>
 * </ul>
 *
 * <p>This design allows for efficient querying of state by key and namespace while maintaining
 * good distribution of data across DynamoDB partitions.
 *
 * <p>Note: This state backend is currently under development and not yet recommended for
 * production use.
 *
 * @param <K> The type of the keys in the state.
 */
@SuppressWarnings("unchecked")
public class DynamoDbKeyedStateBackend<K> extends AwsKeyedStateBackendBase<K, DynamoDbClient, TableDescription> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbKeyedStateBackend.class);

    /** Metrics for monitoring DynamoDB state backend operations. */
    private final DynamoDbStateBackendMetrics metrics;

    /**
     * Creates a new DynamoDBKeyedStateBackend.
     *
     * <p>This constructor initializes the state backend with all required components
     * for storing and retrieving state data from DynamoDB.
     *
     * @param dynamoDbClient The DynamoDB client for state operations
     * @param table The DynamoDB table description for the table storing state
     * @param jobInfo The information about the current job, including job ID and name
     * @param taskInfo The information about the current task, including task name and index
     * @param operatorIdentifier The identifier for the operator that uses this state backend
     * @param kvStateRegistry The registry for key-value state used for queryable state
     * @param keySerializer The serializer for the state keys
     * @param userCodeClassLoader The class loader for user code to ensure proper class resolution
     * @param executionConfig The execution configuration with serialization settings
     * @param ttlTimeProvider The provider for time-to-live functionality in state
     * @param latencyTrackingStateConfig The configuration for tracking state access latency
     * @param cancelStreamRegistry The registry for streams that should be closed on cancellation
     * @param sharedKeyBuilder The builder for serialized composite keys
     * @param compressionDecorator The decorator for stream compression during state serialization
     * @param priorityQueueFactory The factory for creating priority queue state
     * @param keyContext The key context for managing key groups
     * @param registeredPQStates The map of registered priority queue states
     * @param checkpointSnapshotStrategy The strategy for creating state snapshots during checkpoints
     * @param kvStateInformation The map of registered key-value state information
     * @param keyGroupPrefixBytes The number of bytes used for key group prefixes
     * @param useCompression Flag indicating whether to use compression for state data
     */
    public DynamoDbKeyedStateBackend(
            final DynamoDbClient dynamoDbClient,
            final TableDescription table,
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final String operatorIdentifier,
            final TaskKvStateRegistry kvStateRegistry,
            final TypeSerializer<K> keySerializer,
            final ClassLoader userCodeClassLoader,
            final ExecutionConfig executionConfig,
            final TtlTimeProvider ttlTimeProvider,
            final LatencyTrackingStateConfig latencyTrackingStateConfig,
            final CloseableRegistry cancelStreamRegistry,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StreamCompressionDecorator compressionDecorator,
            final PriorityQueueSetFactory priorityQueueFactory,
            final InternalKeyContext<K> keyContext,
            final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            final AwsSnapshotStrategy<K, DynamoDbClient, TableDescription> checkpointSnapshotStrategy,
            final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final int keyGroupPrefixBytes,
            final boolean useCompression,
            final MetricGroup metricGroup) {
        super(
                dynamoDbClient,
                table,
                jobInfo,
                taskInfo,
                operatorIdentifier,
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                sharedKeyBuilder,
                compressionDecorator,
                priorityQueueFactory,
                keyContext,
                registeredPQStates,
                kvStateInformation,
                checkpointSnapshotStrategy,
                keyGroupPrefixBytes,
                useCompression,
                metricGroup);

        // Initialize metrics
        this.metrics = new DynamoDbStateBackendMetrics(metricGroup);
    }

    /**
     * Queries a state entry from DynamoDB.
     *
     * <p>This method retrieves a specific state entry identified by the state name,
     * current key, and namespace from DynamoDB. If the state has TTL enabled, it also
     * checks for expiration and potentially updates the TTL value.
     *
     * <p>The method handles compression by automatically decompressing the retrieved data
     * using Snappy compression.
     *
     * <p>TTL handling:
     * <ul>
     *   <li>If the state is expired and visibility is set to ReturnExpiredIfNotCleanedUp,
     *       the method returns null</li>
     *   <li>If the state is not expired and update type is OnReadAndWrite, the TTL value
     *       is updated in DynamoDB</li>
     * </ul>
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to query
     * @param <N> The type of the namespace
     * @return The serialized state value, or null if the state entry does not exist or is expired
     * @throws IOException If an error occurs during the query operation
     */
    @Override
    public <N> byte[] queryState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace) throws IOException {
        LOG.debug(
                "Querying state '{}' for key '{}' and namespace '{}' from DynamoDB",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final Map<String, AttributeValue> dynamoDbKey = DynamoDbKeyAdapter.createDynamoDbKey(
                    jobInfo,
                    taskInfo,
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);

            LOG.trace("DynamoDB key created: {}", dynamoDbKey);

            final GetItemRequest getItemRequest = GetItemRequest
                    .builder()
                    .tableName(awsModel.tableName())
                    .key(dynamoDbKey)
                    .build();

            LOG.trace("Executing GetItem request to DynamoDB table: {}", awsModel.tableName());
            final long startTime = System.currentTimeMillis();
            final GetItemResponse response = awsClient.getItem(getItemRequest);
            final long endTime = System.currentTimeMillis();
            final Map<String, AttributeValue> item = response.item();

            if (item == null || item.isEmpty()) {
                LOG.debug(
                        "No item found in DynamoDB for state '{}', key '{}', namespace '{}'",
                        stateDesc.getName(),
                        getCurrentKey(),
                        currentNamespace);
                return null;
            }

            LOG.trace("Item retrieved from DynamoDB: {}", item);

            // Check the descriptor to determine if TTL is relevant
            if (stateDesc.getTtlConfig().isEnabled()) {
                LOG.debug(
                        "TTL is enabled for state '{}', checking expiration",
                        stateDesc.getName());

                // Determine if the state is expired
                final AttributeValue ttlAttr = item.get(DynamoDbAttributes.TTL_ATTR);
                final long ttlEpochSeconds = Long.parseLong(ttlAttr.n());
                final long currentEpochSeconds = ttlTimeProvider.currentTimestamp() / 1000;
                final boolean isExpired = ttlEpochSeconds < currentEpochSeconds;

                LOG.trace(
                        "TTL check: current={}, expiration={}, isExpired={}",
                        currentEpochSeconds,
                        ttlEpochSeconds,
                        isExpired);

                if (isExpired && StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp.equals(
                        stateDesc.getTtlConfig().getStateVisibility())) {
                    LOG.debug(
                            "State '{}' is expired and visibility is set to ReturnExpiredIfNotCleanedUp, returning null",
                            stateDesc.getName());
                    // Return expired if not cleaned up
                    return null;
                } else if (!isExpired && StateTtlConfig.UpdateType.OnReadAndWrite.equals(stateDesc
                        .getTtlConfig()
                        .getUpdateType())) {
                    // Update the TTL
                    final long newTtlEpochSeconds =
                            ttlEpochSeconds + stateDesc.getTtlConfig().getTimeToLive().getSeconds();
                    LOG.debug(
                            "Updating TTL for state '{}' from {} to {}",
                            stateDesc.getName(),
                            ttlEpochSeconds,
                            newTtlEpochSeconds);

                    awsClient.updateItem(UpdateItemRequest
                            .builder()
                            .key(dynamoDbKey)
                            .updateExpression("SET #ttl = :ttl")
                            .expressionAttributeNames(ImmutableMap.of(
                                    "#ttl",
                                    DynamoDbAttributes.TTL_ATTR))
                            .expressionAttributeValues(ImmutableMap.of(
                                    ":ttl",
                                    AttributeValue
                                            .builder()
                                            .n(String.valueOf(newTtlEpochSeconds))
                                            .build()))
                            .build());
                    LOG.trace("TTL updated successfully");
                }
            }

            final AttributeValue valueAttr = item.get(DynamoDbAttributes.VALUE_ATTR);
            if (valueAttr == null || valueAttr.b() == null) {
                LOG.debug(
                        "No value attribute found in DynamoDB item for state '{}'",
                        stateDesc.getName());
                return null;
            }

            LOG.trace("Uncompressing state value using Snappy");
            final byte[] result = Snappy.uncompress(valueAttr.b().asByteArray());
            LOG.trace(
                    "Value uncompressed successfully, original size: {}, uncompressed size: {}",
                    valueAttr.b().asByteArray().length,
                    result.length);

            // Report metrics for the read operation
            metrics.reportRead(result.length, endTime - startTime);

            LOG.debug(
                    "State '{}' retrieved successfully for key '{}' and namespace '{}'",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);
            return result;

        } catch (final Exception e) {
            LOG.error(
                    "Error processing state value for state '{}', key '{}', namespace '{}'",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            metrics.reportError();
            throw new IOException("Error processing state value", e);
        }
    }

    /**
     * Inserts or updates a state entry in DynamoDB.
     *
     * <p>This method stores a specific state entry identified by the state name,
     * current key, and namespace in DynamoDB. If an entry with the same identifier already exists,
     * it is overwritten.
     *
     * <p>The method handles compression by automatically compressing the data
     * using Snappy compression before storing it in DynamoDB.
     *
     * <p>In addition to the state value, the method stores metadata including:
     * <ul>
     *   <li>TTL value (if TTL is enabled)</li>
     *   <li>State descriptor name</li>
     *   <li>Job ID and name</li>
     *   <li>Operator ID</li>
     *   <li>Task name and index</li>
     *   <li>Namespace and key information</li>
     * </ul>
     *
     * <p>This metadata is useful for debugging and monitoring purposes.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to insert
     * @param serializedValue The serialized state value to store
     * @param <N> The type of the namespace
     * @throws IOException If an error occurs during the insertion operation
     */
    public <N> void insertState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace,
            final ByteBuffer serializedValue) throws IOException {
        LOG.debug(
                "Inserting state '{}' for key '{}' and namespace '{}' to DynamoDB",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final Map<String, AttributeValue> item = new HashMap<>(DynamoDbKeyAdapter.createDynamoDbKey(
                    jobInfo,
                    taskInfo,
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace));

            LOG.trace("DynamoDB key created for insert: {}", item);

            final AttributeValue.Builder valueBuilder = AttributeValue.builder();
            try {
                LOG.trace(
                        "Compressing state value using Snappy, original size: {}",
                        serializedValue.array().length);
                final byte[] compressed = Snappy.compress(serializedValue.array());
                valueBuilder.b(SdkBytes.fromByteArray(compressed));
                LOG.trace(
                        "Value compressed successfully, compressed size: {}, compression ratio: {}",
                        compressed.length,
                        String.format(
                                "%.2f",
                                (double) compressed.length / serializedValue.array().length));
            } catch (Exception e) {
                LOG.error(
                        "Error compressing state value for state '{}', key '{}', namespace '{}'",
                        stateDesc.getName(),
                        getCurrentKey(),
                        currentNamespace,
                        e);
                throw new IOException("Error compressing state value", e);
            }

            item.put(DynamoDbAttributes.VALUE_ATTR, valueBuilder.build());

            // Configure TTL, if enabled
            if (stateDesc.getTtlConfig().isEnabled()) {
                LOG.debug(
                        "Setting TTL for state '{}' with TTL config: {}",
                        stateDesc.getName(),
                        stateDesc.getTtlConfig());

                final long currentTimestamp = ttlTimeProvider.currentTimestamp();
                final long ttlValue = stateDesc
                        .getTtlConfig()
                        .getTimeToLive()
                        .plusMillis(currentTimestamp)
                        .getSeconds();

                LOG.trace(
                        "TTL calculation: currentTimestamp={}, ttlSeconds={}, expirationTimestamp={}",
                        currentTimestamp,
                        stateDesc.getTtlConfig().getTimeToLive().getSeconds(),
                        ttlValue);

                item.put(
                        DynamoDbAttributes.TTL_ATTR,
                        AttributeValue.builder().n(String.valueOf(ttlValue)).build());
            }

            // Additional attributes
            item.put(
                    DynamoDbAttributes.STATE_DESCRIPTOR_ATTR,
                    AttributeValue.builder().s(stateDesc.getName()).build());
            item.put(
                    DynamoDbAttributes.JOB_ID_ATTR,
                    AttributeValue.builder().s(jobInfo.getJobId().toString()).build());
            item.put(
                    DynamoDbAttributes.OPERATOR_ID_ATTR,
                    AttributeValue.builder().s(operatorIdentifier).build());
            item.put(
                    DynamoDbAttributes.JOB_NAME_ATTR,
                    AttributeValue.builder().s(jobInfo.getJobName()).build());
            // Add TaskInfo metadata
            item.put(
                    DynamoDbAttributes.TASK_NAME_ATTR,
                    AttributeValue.builder().s(taskInfo.getTaskName()).build());
            item.put(
                    DynamoDbAttributes.TASK_INDEX_ATTR,
                    AttributeValue
                            .builder()
                            .n(String.valueOf(taskInfo.getIndexOfThisSubtask()))
                            .build());

//        final boolean ambiguousKeyPossible = CompositeKeySerializationUtils.isAmbiguousKeyPossible(
//                getKeySerializer(),
//                kvStateInformation
//                        .get(state)
//                        .getNamespaceSerializer());
//        final Object namespace = CompositeKeySerializationUtils.readNamespace(
//                kvStateInformation
//                        .get(state)
//                        .getNamespaceSerializer(),
//                new DataInputDeserializer(serializedKeyAndNamespace),
//                ambiguousKeyPossible);
//        final K key = CompositeKeySerializationUtils.readKey(
//                getKeySerializer(),
//                new DataInputDeserializer(serializedKeyAndNamespace),
//                ambiguousKeyPossible);

            item.put(
                    DynamoDbAttributes.NAMESPACE_ATTR,
                    AttributeValue.builder().s(currentNamespace.toString()).build());
            item.put(
                    DynamoDbAttributes.STATE_KEY_ATTR,
                    AttributeValue.builder().s(getCurrentKey().toString()).build());

            final PutItemRequest putItemRequest = PutItemRequest
                    .builder()
                    .tableName(awsModel.tableName())
                    .item(item)
                    .build();

            LOG.trace("Executing PutItem request to DynamoDB table: {}", awsModel.tableName());
            final long startTime = System.currentTimeMillis();
            awsClient.putItem(putItemRequest);
            final long endTime = System.currentTimeMillis();

            // Report metrics for the write operation
            metrics.reportWrite(serializedValue.array().length, endTime - startTime);

            LOG.debug(
                    "State '{}' inserted successfully for key '{}' and namespace '{}'",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);
        } catch (Exception e) {
            LOG.error(
                    "Error inserting state '{}' for key '{}' and namespace '{}' to DynamoDB",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            metrics.reportError();
            throw new IOException("Error inserting state to DynamoDB", e);
        }
    }

    /**
     * Deletes a state entry from DynamoDB.
     *
     * <p>This method removes a specific state entry identified by the state name,
     * current key, and namespace from DynamoDB.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to delete
     * @param <N> The type of the namespace
     * @throws IOException If an error occurs during the deletion operation
     */
    @Override
    public <N> void deleteState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace) throws IOException {
        LOG.debug(
                "Deleting state '{}' for key '{}' and namespace '{}' from DynamoDB",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final Map<String, AttributeValue> key = DynamoDbKeyAdapter.createDynamoDbKey(
                    jobInfo,
                    taskInfo,
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);

            LOG.trace("DynamoDB key created for delete: {}", key);

            final DeleteItemRequest deleteItemRequest = DeleteItemRequest
                    .builder()
                    .tableName(awsModel.tableName())
                    .key(key)
                    .build();

            LOG.trace("Executing DeleteItem request to DynamoDB table: {}", awsModel.tableName());
            awsClient.deleteItem(deleteItemRequest);
            LOG.debug(
                    "State '{}' deleted successfully for key '{}' and namespace '{}'",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);
        } catch (Exception e) {
            LOG.error(
                    "Error deleting state '{}' for key '{}' and namespace '{}' from DynamoDB",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            throw new IOException("Error deleting state from DynamoDB", e);
        }
    }

    /**
     * Creates an iterator over all state entries for a specific state descriptor.
     *
     * <p>This method returns an iterator that can be used to traverse all state entries
     * for a given state descriptor. It uses DynamoDB's BatchGetItem API to efficiently
     * retrieve multiple items in a single request.
     *
     * <p>The iterator handles pagination of DynamoDB results and deserializes the state entries
     * on demand as they are accessed.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param <N> The type of the namespace
     * @param <SV> The type of the state value
     * @return An iterator over all state entries for the specified state
     */
    @Override
    public <N, SV> AwsStateIterator<K, N, SV, ?> stateIterator(
            final StateDescriptor<?, ?> stateDesc) {
        LOG.debug("Creating state iterator for state '{}' from DynamoDB", stateDesc.getName());

        final RegisteredKeyValueStateBackendMetaInfo<N, SV> stateInfo = (RegisteredKeyValueStateBackendMetaInfo<N, SV>) this.kvStateInformation.get(
                stateDesc.getName());

        // Use GSI query to find all states for this job, task, and state descriptor
        // Using Query operation with GSI: jobNameAndTaskName + state_descriptor
        final String jobNameAndTaskNameValue = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName());

        return new AwsStateIterator<>(
                awsClient.queryPaginator(r -> r.tableName(awsModel.tableName())
                        .indexName(DynamoDbAttributes.GSI_JOB_NAME_TASK_NAME_INDEX)
                        .keyConditionExpression("#jntn = :jntnValue AND #sd = :sdValue")
                        .expressionAttributeNames(ImmutableMap.of(
                                "#jntn", DynamoDbAttributes.JOB_NAME_AND_TASK_NAME_ATTR,
                                "#sd", DynamoDbAttributes.STATE_DESCRIPTOR_ATTR))
                        .expressionAttributeValues(ImmutableMap.of(
                                ":jntnValue", AttributeValue.builder().s(jobNameAndTaskNameValue).build(),
                                ":sdValue", AttributeValue.builder().s(stateInfo.getName()).build()))
                        .build()),
                response -> response
                        .items()
                        .stream()
                        .map(dynamoKey -> {
                            try {
                                // Get state key and namespace directly from attributes
                                final String stateKey = DynamoDbKeyAdapter.getStateKey(dynamoKey);
                                final String namespaceStr = dynamoKey.get(DynamoDbAttributes.NAMESPACE_ATTR).s();

                                // Deserialize key and namespace using the appropriate serializers
                                final DataInputDeserializer keyDeserializer = new DataInputDeserializer(stateKey.getBytes());
                                final DataInputDeserializer namespaceDeserializer = new DataInputDeserializer(namespaceStr.getBytes());

                                final K key = keySerializer.deserialize(keyDeserializer);
                                final N namespace = stateInfo.getNamespaceSerializer().deserialize(namespaceDeserializer);

                                // Get value using key and namespace
                                final byte[] valueBytes = queryState(stateDesc, namespace);
                                return new StateEntry.SimpleStateEntry<>(
                                        key,
                                        namespace,
                                        stateInfo
                                                .getStateSerializer()
                                                .deserialize(new DataInputDeserializer(valueBytes)));
                            } catch (final IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .iterator());
    }
}
