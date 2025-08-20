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

package org.apache.flink.state.backend.aws.s3.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.state.backend.aws.s3.S3KeyedStateBackend;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.backend.aws.s3.snapshot.S3SnapshotMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * Implementation of {@link SnapshotStrategy} for the S3 state backend.
 *
 * <p>This class is responsible for creating snapshots of the keyed state stored in S3
 * during Flink's checkpointing process. It implements both {@link SnapshotStrategy} and
 * {@link CheckpointListener} interfaces to integrate with Flink's checkpointing mechanism.
 *
 * <p>The snapshot strategy works by:
 * <ol>
 *   <li>Collecting metadata about all registered states</li>
 *   <li>Serializing this metadata and storing it in S3</li>
 *   <li>Creating a state handle that can be used to restore the state</li>
 * </ol>
 *
 * <p>The strategy uses an incremental approach where only metadata is stored during checkpoints,
 * as the actual state data remains in S3 and is accessed directly during restoration.
 *
 * @param <K> The type of the key.
 *
 * @see SnapshotStrategy
 * @see CheckpointListener
 */
public class S3SnapshotStrategy<K> extends AwsSnapshotStrategy<K, S3Client, Bucket> {

    private static final Logger LOG = LoggerFactory.getLogger(S3SnapshotStrategy.class);

    /**
     * Metrics for monitoring S3 snapshot operations.
     */
    private final S3SnapshotMetrics metrics;

    /**
     * Creates a new S3SnapshotStrategy.
     *
     * @param s3Client The S3 client for interacting with the S3 service
     * @param bucket The S3 bucket where state is stored
     * @param backendUID The unique identifier for this state backend instance
     * @param keyGroupRange The key-group range for the task
     * @param localRecoveryConfig The configuration for local recovery
     * @param keySerializer The key serializer of the backend
     * @param kvStateInformation Key/Value state meta info from the backend
     */
    public S3SnapshotStrategy(
            final S3Client s3Client,
            final Bucket bucket,
            @Nonnull final UUID backendUID,
            @Nonnull final KeyGroupRange keyGroupRange,
            @Nonnull final LocalRecoveryConfig localRecoveryConfig,
            @Nonnull final TypeSerializer<K> keySerializer,
            @Nonnull final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation) {
        this(s3Client, bucket, backendUID, keyGroupRange, localRecoveryConfig, keySerializer, kvStateInformation, null);
    }

    public S3SnapshotStrategy(
            final S3Client s3Client,
            final Bucket bucket,
            @Nonnull final UUID backendUID,
            @Nonnull final KeyGroupRange keyGroupRange,
            @Nonnull final LocalRecoveryConfig localRecoveryConfig,
            @Nonnull final TypeSerializer<K> keySerializer,
            @Nonnull final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final MetricGroup metricGroup) {
        super(
                s3Client,
                bucket,
                backendUID,
                keyGroupRange,
                localRecoveryConfig,
                keySerializer,
                kvStateInformation,
                metricGroup);

        this.metrics = new S3SnapshotMetrics(this.metricGroup);
    }

    /**
     * Notifies that a checkpoint has been completed.
     *
     * <p>This method is called when a checkpoint has been confirmed as completed.
     * For S3, no additional action is needed as the state data remains in the bucket.
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
     * metadata entry for the aborted checkpoint from S3 to clean up resources.
     *
     * <p>The method logs timing information for the delete operation for performance monitoring.
     *
     * @param checkpointId The ID of the aborted checkpoint
     * @throws Exception If an error occurs during notification
     */

    @Override
    public void notifyCheckpointAborted(final long checkpointId) throws Exception {
        // Start timing the checkpoint abort operation
        final long startTime = System.nanoTime();

        try {
            awsClient.deleteObject(DeleteObjectRequest.builder()
                    .bucket(awsModel.name())
                    .key(getCheckpointPrefix(checkpointId))
                    .build());

            // Calculate delete duration in milliseconds
            final long deleteDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report checkpoint abort metrics
            metrics.reportCheckpointAbort(deleteDurationMs);

            LOG.info(
                    "S3 checkpoint abort completed in {} ms, checkpoint ID: {}",
                    deleteDurationMs, checkpointId);
        } catch (Exception e) {
            metrics.reportError();
            throw e;
        }
    }

    /**
     * Persists the serialized snapshot data to S3.
     *
     * <p>This method stores the serialized state metadata in S3 using a checkpoint-specific key.
     * It uploads the data to S3, then verifies the upload by checking the object's existence
     * and size using a HEAD request.
     *
     * <p>The method logs detailed timing information for performance monitoring, including
     * the duration of the upload and HEAD operations.
     *
     * @param checkpointId The ID of the checkpoint
     * @param serializedSnapshots The serialized snapshot data to persist
     * @return The size of the persisted data in bytes
     */
    @Override
    public long persist(long checkpointId, byte[] serializedSnapshots) {
        // Construct the S3 key for the checkpoint metadata
        final String checkpointPrefix = getCheckpointPrefix(checkpointId);

        // Start timing the checkpoint persist operation
        final long startTime = System.nanoTime();

        try {
            // Upload the serialized metadata to S3
            awsClient.putObject(
                    PutObjectRequest
                            .builder()
                            .bucket(awsModel.name())
                            .key(checkpointPrefix)
                            .build(),
                    RequestBody.fromBytes(serializedSnapshots));

            // Calculate upload duration in milliseconds
            final long uploadDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report upload metrics
            metrics.reportUpload(uploadDurationMs);

            // Start timing the head object operation
            final long headStartTime = System.nanoTime();

            // Check that the object exists and get its size
            HeadObjectResponse headObjectResponse = awsClient.headObject(
                    HeadObjectRequest.builder()
                            .bucket(awsModel.name())
                            .key(checkpointPrefix)
                            .build());

            // Calculate head operation duration in milliseconds
            final long headDurationMs = (System.nanoTime() - headStartTime) / 1_000_000;

            // Report head metrics
            metrics.reportHead(headDurationMs);

            // Calculate total operation duration
            final long totalDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report overall checkpoint metrics
            metrics.reportCheckpoint(headObjectResponse.contentLength(), totalDurationMs);

            LOG.info(
                    "S3 checkpoint persist completed in {} ms (upload: {} ms, head: {} ms), " +
                            "checkpoint ID: {}, size: {} bytes",
                    totalDurationMs, uploadDurationMs, headDurationMs,
                    checkpointId, headObjectResponse.contentLength());

            return headObjectResponse.contentLength();
        } catch (Exception e) {
            metrics.reportError();
            throw e;
        }
    }

    /**
     * Gets the S3 key prefix for a checkpoint.
     *
     * <p>This method constructs the S3 key prefix for a checkpoint based on the checkpoint ID.
     * The prefix follows the pattern: {@code checkpoints/[checkpointId]}
     *
     * @param checkpointId The ID of the checkpoint
     * @return The S3 key prefix for the checkpoint
     */
    private static String getCheckpointPrefix(long checkpointId) {
        return "checkpoints/" + checkpointId;
    }
}
