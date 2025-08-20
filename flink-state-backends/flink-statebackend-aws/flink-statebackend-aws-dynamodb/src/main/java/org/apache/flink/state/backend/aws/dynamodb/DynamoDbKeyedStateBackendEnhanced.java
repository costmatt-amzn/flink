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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbAttributes;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;
import org.apache.flink.state.backend.aws.dynamodb.metrics.DynamoDbStateBackendMetrics;
import org.apache.flink.state.backend.aws.dynamodb.model.StateItem;
import org.apache.flink.state.backend.aws.dynamodb.util.DynamoDbEnhancedClientUtil;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.state.backend.aws.state.AwsStateIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link org.apache.flink.runtime.state.KeyedStateBackend} that stores its state in AWS DynamoDB
 * using the DynamoDB Enhanced Client.
 *
 * <p>This implementation uses the DynamoDB Enhanced Client to provide a more object-oriented
 * approach to working with DynamoDB, with automatic mapping between Java objects and DynamoDB items.
 *
 * @param <K> The type of the keys in the state.
 */
@SuppressWarnings("unchecked")
public class DynamoDbKeyedStateBackendEnhanced<K> extends AwsKeyedStateBackendBase<K, DynamoDbClient, TableDescription> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbKeyedStateBackendEnhanced.class);

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<StateItem> stateTable;

    /** Metrics for monitoring DynamoDB state backend operations. */
    private final DynamoDbStateBackendMetrics metrics;
    /**
     * Creates a new DynamoDBKeyedStateBackendEnhanced.
     */
    public DynamoDbKeyedStateBackendEnhanced(
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

        this.metrics = new DynamoDbStateBackendMetrics(metricGroup);
        this.enhancedClient = DynamoDbEnhancedClientUtil.createEnhancedClient(dynamoDbClient);
        this.stateTable = DynamoDbEnhancedClientUtil.createStateItemTable(
                enhancedClient,
                table.tableName());
    }

    @Override
    public <N> byte[] queryState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace) throws IOException {
        LOG.debug(
                "Querying state '{}' for key '{}' and namespace '{}' from DynamoDB using Enhanced Client",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final Key key = DynamoDbEnhancedClientUtil.createKey(
                    jobInfo,
                    taskInfo,
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);

            LOG.trace(
                    "DynamoDB Enhanced key. partitionKey: {}, sortKey: {}",
                    key.partitionKeyValue(),
                    key.sortKeyValue());

            LOG.trace("Executing getItem request to DynamoDB table: {}", awsModel.tableName());
            final StateItem item = stateTable.getItem(key);

            if (item == null || item.getValue() == null) {
                LOG.debug(
                        "No item found in DynamoDB for state '{}', key '{}', namespace '{}'",
                        stateDesc.getName(),
                        getCurrentKey(),
                        currentNamespace);
                return null;
            }

            LOG.trace("Item retrieved from DynamoDB: {}", item);

            // Check the descriptor to determine if TTL is relevant
            if (stateDesc.getTtlConfig().isEnabled() && item.getTtl() != null) {
                LOG.debug(
                        "TTL is enabled for state '{}', checking expiration",
                        stateDesc.getName());

                // Determine if the state is expired
                final long ttlEpochSeconds = item.getTtl();
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

                    // Since StateItem is now immutable, create a new instance with updated TTL
                    final StateItem updatedItem = StateItem.builder()
                            .jobAwareStateKeyAndDescriptor(item.getJobAwareStateKeyAndDescriptor())
                            .namespace(item.getNamespace())
                            .jobNameAndTaskName(item.getJobNameAndTaskName())
                            .value(item.getValue())
                            .ttl(newTtlEpochSeconds)
                            .stateDescriptor(item.getStateDescriptor())
                            .jobId(item.getJobId())
                            .jobName(item.getJobName())
                            .taskName(item.getTaskName())
                            .taskIndex(item.getTaskIndex())
                            .operatorId(item.getOperatorId())
                            .stateKey(item.getStateKey())
                            .build();

                    LOG.trace("Updating item with new TTL");
                    stateTable.updateItem(updatedItem);
                    LOG.trace("TTL updated successfully");
                }
            }

            final byte[] valueBytes = item.getValue().asByteArray();

            LOG.trace("Uncompressing state value using Snappy");
            final byte[] result = Snappy.uncompress(valueBytes);
            LOG.trace(
                    "Value uncompressed successfully, original size: {}, uncompressed size: {}",
                    valueBytes.length,
                    result.length);

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
            throw new IOException("Error processing state value", e);
        }
    }

    public <N> void insertState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace, final ByteBuffer serializedValue) throws IOException {
        LOG.debug(
                "Inserting state '{}' for key '{}' and namespace '{}' to DynamoDB using Enhanced Client",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        // Calculate TTL if enabled
        Long ttl = null;
        if (stateDesc.getTtlConfig().isEnabled()) {
            LOG.debug(
                    "Setting TTL for state '{}' with TTL config: {}",
                    stateDesc.getName(),
                    stateDesc.getTtlConfig());

            ttl = stateDesc
                    .getTtlConfig()
                    .getTimeToLive()
                    .plusMillis(ttlTimeProvider.currentTimestamp())
                    .getSeconds();

            LOG.trace(
                    "TTL calculation: currentTimestamp={}, ttlSeconds={}, expirationTimestamp={}",
                    ttlTimeProvider.currentTimestamp(),
                    stateDesc.getTtlConfig().getTimeToLive().getSeconds(),
                    ttl);
        }

        try {
            // Use the key adapter to create a consistent key structure
            LOG.trace("Creating StateItem for insert");
            final StateItem item = DynamoDbEnhancedClientUtil.createStateItem(
                    jobInfo,
                    taskInfo,
                    operatorIdentifier,
                    stateDesc.getName(),
                    serializedValue.array(),
                    ttl,
                    getCurrentKey(),
                    currentNamespace
            );

            LOG.trace("Executing putItem request to DynamoDB table: {}", awsModel.tableName());
            stateTable.putItem(item);
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
            throw new IOException("Error inserting state to DynamoDB", e);
        }
    }

    @Override
    public <N> void deleteState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace) throws IOException {
        LOG.debug(
                "Deleting state '{}' for key '{}' and namespace '{}' from DynamoDB using Enhanced Client",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final Key key = DynamoDbEnhancedClientUtil.createKey(
                    jobInfo,
                    taskInfo,
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace);

            LOG.trace("DynamoDB Enhanced key created for delete: {}", key);

            LOG.trace("Executing deleteItem request to DynamoDB table: {}", awsModel.tableName());
            stateTable.deleteItem(key);
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

    @Override
    public <N, SV> AwsStateIterator<K, N, SV, ?> stateIterator(final StateDescriptor<?, ?> stateDesc) {
        LOG.debug(
                "Creating state iterator for state '{}' from DynamoDB using Enhanced Client",
                stateDesc.getName());

        final RegisteredKeyValueStateBackendMetaInfo<N, SV> stateInfo = (RegisteredKeyValueStateBackendMetaInfo<N, SV>) this.kvStateInformation.get(
                stateDesc.getName());

        // Use the GSI to query by job and task with state descriptor
        LOG.trace("Building QueryEnhancedRequest for state '{}' using GSI", stateDesc.getName());

        final String jobNameAndTaskNameValue = String.join(
                DynamoDbStateBackendConstants.KEY_SEPARATOR,
                jobInfo.getJobName(),
                taskInfo.getTaskName());

        // Create a query request for the GSI
        QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
                .queryConditional(QueryConditional.keyEqualTo(Key.builder()
                        .partitionValue(jobNameAndTaskNameValue)
                        .sortValue(stateInfo.getName())
                        .build()))
                .build();

        LOG.trace("Executing query request to DynamoDB GSI: {}", DynamoDbAttributes.GSI_JOB_NAME_TASK_NAME_INDEX);

        // Query the GSI
        return new AwsStateIterator<>(
                stateTable.index(DynamoDbAttributes.GSI_JOB_NAME_TASK_NAME_INDEX).query(queryEnhancedRequest),
                response -> {
                    LOG.trace("Processing batch get result page");
                    return response
                            .items()
                            .stream()
                            .map(stateItem -> {
                                LOG.trace("Converting StateItem to StateEntry: {}", stateItem);
                                return DynamoDbEnhancedClientUtil.toStateEntry(
                                        getKeySerializer(),
                                        stateInfo.getNamespaceSerializer(),
                                        stateInfo.getStateSerializer(),
                                        stateItem);
                            })
                            .iterator();
                });
    }
}
