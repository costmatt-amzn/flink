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

package org.apache.flink.state.backend.aws.dynamodb.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbAttributes;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * Implementation of {@link SnapshotStrategy} for the DynamoDB state backend.
 *
 * <p>This class is responsible for creating snapshots of the keyed state stored in DynamoDB
 * during Flink's checkpointing process. It implements both {@link SnapshotStrategy} and
 * {@link CheckpointListener} interfaces to integrate with Flink's checkpointing mechanism.
 *
 * <p>The snapshot strategy works by:
 * <ol>
 *   <li>Collecting metadata about all registered states</li>
 *   <li>Serializing this metadata and storing it in DynamoDB</li>
 *   <li>Creating a state handle that can be used to restore the state</li>
 * </ol>
 *
 * <p>The strategy uses an incremental approach where only metadata is stored during checkpoints,
 * as the actual state data remains in DynamoDB and is accessed directly during restoration.
 * This approach minimizes the amount of data that needs to be transferred during checkpointing,
 * making the process more efficient.
 *
 * <p>When a checkpoint is aborted, the strategy cleans up any metadata stored for that checkpoint
 * to avoid resource leaks.
 *
 * @param <K> The type of the key.
 * @see SnapshotStrategy
 * @see CheckpointListener
 */
public class DynamoDbSnapshotStrategy<K> extends AwsSnapshotStrategy<K, DynamoDbClient, TableDescription> {

    /**
     * Metrics for monitoring DynamoDB snapshot operations.
     */
    private final DynamoDbSnapshotMetrics metrics;

    /**
     * Creates a new DynamoDbSnapshotStrategy.
     *
     * @param dynamoDbClient      The DynamoDB client for interacting with the DynamoDB service
     * @param backendUID          The unique identifier for this state backend instance
     * @param keyGroupRange       The key-group range for the task
     * @param localRecoveryConfig The configuration for local recovery
     * @param keySerializer       The key serializer of the backend
     * @param kvStateInformation  Key/Value state meta info from the backend
     */
    public DynamoDbSnapshotStrategy(
            final DynamoDbClient dynamoDbClient,
            final TableDescription table,
            @Nonnull final UUID backendUID,
            @Nonnull final KeyGroupRange keyGroupRange,
            @Nonnull final LocalRecoveryConfig localRecoveryConfig,
            @Nonnull final TypeSerializer<K> keySerializer,
            @Nonnull final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final MetricGroup metricGroup) {
        super(dynamoDbClient, table, backendUID, keyGroupRange, localRecoveryConfig, keySerializer, kvStateInformation, metricGroup);
        this.metrics = new DynamoDbSnapshotMetrics(this.metricGroup);
    }

    /**
     * Persists the serialized snapshot data to DynamoDB.
     *
     * <p>This method stores the serialized state metadata in DynamoDB using the checkpoint ID
     * as the partition key. The data is stored as a binary attribute to preserve the exact
     * serialization format.
     *
     * @param checkpointId The ID of the checkpoint
     * @param serializedSnapshots The serialized snapshot data to persist
     * @return The estimated size of the persisted data in bytes
     */
    @Override
    public long persist(final long checkpointId, final byte[] serializedSnapshots) {
        // Start timing the checkpoint persist operation
        final long startTime = System.nanoTime();

        try {
            final PutItemRequest putItemRequest = PutItemRequest
                    .builder()
                    .tableName(awsModel.tableName())
                    .item(ImmutableMap.of(
                            DynamoDbAttributes.JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR,
                            AttributeValue.builder().s(String.valueOf(checkpointId)).build(),
                            DynamoDbAttributes.VALUE_ATTR,
                            AttributeValue
                                    .builder()
                                    .b(SdkBytes.fromByteArray(serializedSnapshots))
                                    .build()))
                    .build();
            final PutItemResponse putItemResponse = awsClient.putItem(putItemRequest);

            // Calculate total operation duration in milliseconds
            final long totalDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Get the size estimate
            long sizeEstimate = (long) (putItemResponse.itemCollectionMetrics()
                    .sizeEstimateRangeGB()
                    .get(0) * FileUtils.ONE_GB);

            // Report checkpoint metrics
            metrics.reportCheckpoint(sizeEstimate, totalDurationMs);

            return sizeEstimate;
        } catch (Exception e) {
            metrics.reportError();
            throw e;
        }

    }

    /**
     * Notifies that a checkpoint has been completed.
     *
     * <p>This method is called when a checkpoint has been confirmed as completed.
     * For DynamoDB, no additional action is needed as the state data remains in the database.
     *
     * @param checkpointId The ID of the completed checkpoint
     * @throws Exception If an error occurs during notification
     */

    @Override
    public void notifyCheckpointComplete(final long checkpointId) throws Exception {
        // Report checkpoint complete metrics
        metrics.reportCheckpointComplete();
    }

    /**
     * Notifies that a checkpoint has been aborted.
     *
     * <p>This method is called when a checkpoint has been aborted. It deletes the
     * metadata entry for the aborted checkpoint from DynamoDB to clean up resources.
     *
     * @param checkpointId The ID of the aborted checkpoint
     * @throws Exception If an error occurs during notification
     */

    @Override
    public void notifyCheckpointAborted(final long checkpointId) throws Exception {
        // Start timing the checkpoint abort operation
        final long startTime = System.nanoTime();

        try {
            awsClient.deleteItem(DeleteItemRequest.builder()
                    .tableName(awsModel.tableName())
                    .key(ImmutableMap.of(DynamoDbAttributes.JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR,
                            AttributeValue.builder().s(String.valueOf(checkpointId)).build()))
                    .build());

            // Calculate delete duration in milliseconds
            final long deleteDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report checkpoint abort metrics
            metrics.reportCheckpointAbort(deleteDurationMs);
        } catch (Exception e) {
            metrics.reportError();
            throw e;
        }
    }
}
