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

package org.apache.flink.state.backend.aws.s3;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
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
import org.apache.flink.runtime.taskexecutor.slot.DefaultTimerService;
import org.apache.flink.runtime.taskexecutor.slot.TimeoutListener;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.state.backend.aws.s3.metrics.S3StateBackendMetrics;
import org.apache.flink.state.backend.aws.s3.util.S3KeyAdapter;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.state.backend.aws.state.AwsStateIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link org.apache.flink.runtime.state.KeyedStateBackend} that stores its state in Amazon S3.
 *
 * <p>This state backend implementation uses Amazon S3 as the persistent storage for
 * keyed state in Flink applications. It supports all standard Flink state types (ValueState,
 * ListState, MapState, ReducingState, AggregatingState) and provides exactly-once semantics
 * through the use of checkpoints and savepoints.
 *
 * <p>Key features:
 * <ul>
 *   <li>Durable state storage in S3 with configurable bucket and path settings</li>
 *   <li>Support for all standard Flink state types</li>
 *   <li>Automatic compression of state data using Snappy</li>
 *   <li>TTL (Time-To-Live) support with automatic cleanup of expired state</li>
 *   <li>Performance optimizations including logging of operation durations</li>
 *   <li>Incremental checkpointing support</li>
 * </ul>
 *
 * <p>The state is stored in S3 objects with keys that encode:
 * <ul>
 *   <li>The state name (from the state descriptor)</li>
 *   <li>The key group (derived from the key)</li>
 *   <li>The user key</li>
 *   <li>The namespace</li>
 * </ul>
 *
 * <p>This implementation also supports TTL (Time-To-Live) for state objects. When TTL is enabled
 * for a state descriptor, the state backend will register timers to periodically check for
 * expired objects and delete them from S3.
 *
 * @param <K> The type of the keys in the state.
 */
@SuppressWarnings("unchecked")
public class S3KeyedStateBackend<K>
        extends AwsKeyedStateBackendBase<K, S3Client, Bucket>
        implements TimeoutListener<Tuple3<K, Object, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(S3KeyedStateBackend.class);

    /**
     * The configuration for the S3 state backend.
     */
    private final Configuration configuration;
    private final TimerService<Tuple3<K, Object, String>> timerService;

    /**
     * Metrics for monitoring S3 state backend performance.
     */
    private final S3StateBackendMetrics metrics;

    /**
     * Creates a new S3KeyedStateBackend.
     *
     * <p>This constructor initializes the S3 keyed state backend with all required components
     * for state management. It sets up the key context, serialized composite key builder,
     * and other components needed for state operations.
     *
     * @param s3Client The S3 client for state operations
     * @param bucket The S3 bucket to use for state storage
     * @param jobInfo The information about the current job, including job ID and other metadata
     * @param taskInfo The information about the current task, including task name and index
     * @param operatorIdentifier The identifier for the operator that uses this state backend
     * @param kvStateRegistry The registry for key-value state used for queryable state
     * @param keySerializer The serializer for the state keys
     * @param userCodeClassLoader The class loader for user code to ensure proper class resolution
     * @param executionConfig The execution configuration with serialization settings
     * @param ttlTimeProvider The provider for time-to-live functionality in state
     * @param latencyTrackingStateConfig The configuration for tracking state access latency
     * @param cancelStreamRegistry The registry for streams that should be closed on cancellation
     * @param configuration The configuration containing S3-specific settings
     * @param sharedKeyBuilder The builder for serialized composite keys
     * @param compressionDecorator The decorator for stream compression during state serialization
     * @param priorityQueueFactory The factory for creating priority queue state
     * @param keyContext The key context for managing key groups
     * @param checkpointSnapshotStrategy The strategy for creating state snapshots during checkpoints
     * @param registeredPQStates The map of registered priority queue states
     * @param kvStateInformation The map of registered key-value state information
     * @param keyGroupPrefixBytes The number of bytes used for key group prefixes
     * @param useCompression Flag indicating whether to use compression for state data
     */
    public S3KeyedStateBackend(
            final S3Client s3Client,
            final Bucket bucket,
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
            final Configuration configuration,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StreamCompressionDecorator compressionDecorator,
            final PriorityQueueSetFactory priorityQueueFactory,
            final InternalKeyContext<K> keyContext,
            final AwsSnapshotStrategy<K, S3Client, Bucket> checkpointSnapshotStrategy,
            final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final int keyGroupPrefixBytes,
            final boolean useCompression,
            final MetricGroup metricGroup) {
        super(
                s3Client,
                bucket,
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

        this.configuration = configuration;
        this.timerService = new DefaultTimerService<>(new ScheduledThreadPoolExecutor(1), 1);
        this.timerService.start(this);
        this.metrics = new S3StateBackendMetrics(this.metricGroup);
    }

    /**
     * Deletes a state entry from S3.
     *
     * <p>This method removes a specific state entry identified by the state name,
     * current key, and namespace from S3. It also unregisters any associated TTL timeout.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to delete
     * @param <N> The type of the namespace
     */
    @Override
    public <N> void deleteState(final StateDescriptor<?, ?> stateDesc, final N currentNamespace) {
        LOG.debug(
                "Deleting state '{}' for key '{}' and namespace '{}' from S3",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            // Get the S3 key
            final String s3Key = getS3Key(getCurrentKey(), currentNamespace, stateDesc);

            LOG.trace("S3 key created for delete: {}", s3Key);

            // Delete the value
            LOG.trace("Executing DeleteObject request to S3 bucket: {}", awsModel.name());

            // Start timing the delete operation
            final long startTime = System.nanoTime();

            awsClient.deleteObject(DeleteObjectRequest
                    .builder()
                    .bucket(awsModel.name())
                    .key(s3Key)
                    .build());

            // Calculate delete duration in milliseconds
            final long deleteDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report metrics
            metrics.reportDelete(deleteDurationMs);

            LOG.info("S3 delete operation completed in {} ms, key: {}", deleteDurationMs, s3Key);

            LOG.debug("State '{}' deleted successfully from S3", stateDesc.getName());

            timerService.unregisterTimeout(Tuple3.of(
                    getCurrentKey(),
                    currentNamespace,
                    stateDesc.getName()));
        } catch (final Exception e) {
            // Report error metrics
            metrics.reportError();

            LOG.error(
                    "Error deleting state '{}' for key '{}' and namespace '{}' from S3",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            throw new RuntimeException("Error deleting value from S3", e);
        }
    }

    /**
     * Queries a state entry from S3.
     *
     * <p>This method retrieves a specific state entry identified by the state name,
     * current key, and namespace from S3. If the state has TTL enabled with OnReadAndWrite
     * update type, it also refreshes the TTL timeout.
     *
     * <p>The method handles compression by automatically decompressing the retrieved data
     * using Snappy compression.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to query
     * @param <N> The type of the namespace
     * @return The serialized state value, or null if the state entry does not exist
     */
    @Override
    public <N> byte[] queryState(final StateDescriptor<?, ?> stateDesc, final N currentNamespace) {
        LOG.debug(
                "Querying state '{}' for key '{}' and namespace '{}' from S3",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            final String s3Key = getS3Key(getCurrentKey(), currentNamespace, stateDesc);

            try {
                // Get directly from S3
                LOG.trace(
                        "Executing GetObject request to S3 bucket: {} with key: {}",
                        awsModel.name(),
                        s3Key);

                // Start timing the read operation
                final long startTime = System.nanoTime();

                final ResponseBytes<GetObjectResponse> objectResponse = awsClient.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(awsModel.name()).key(s3Key).build());
                byte[] valueBytes = objectResponse.asByteArray();

                // Calculate read duration in milliseconds
                final long readDurationMs = (System.nanoTime() - startTime) / 1_000_000;

                // Report metrics
                metrics.reportRead(valueBytes.length, readDurationMs);

                LOG.info(
                        "S3 read operation completed in {} ms, object size: {} bytes, key: {}",
                        readDurationMs,
                        valueBytes.length,
                        s3Key);

                // Uncompress if in the configuration
                LOG.trace("Uncompressing state value using Snappy");
                final long startDecompress = System.nanoTime();
                final byte[] uncompressed = Snappy.uncompress(valueBytes);
                final long decompressDurationMs = (System.nanoTime() - startDecompress) / 1_000_000;

                // Report decompression metrics
                metrics.reportDecompression(decompressDurationMs);

                LOG.trace(
                        "Value uncompressed successfully in {} ms, original size: {}, uncompressed size: {}",
                        decompressDurationMs,
                        valueBytes.length,
                        uncompressed.length);
                valueBytes = uncompressed;

                LOG.debug("State '{}' retrieved successfully from S3", stateDesc.getName());

                // Update the ttl manager
                if (stateDesc.getTtlConfig().isEnabled()
                        && StateTtlConfig.UpdateType.OnReadAndWrite.equals(stateDesc
                        .getTtlConfig()
                        .getUpdateType())) {
                    timerService.registerTimeout(
                            Tuple3.of(getCurrentKey(), currentNamespace, stateDesc.getName()),
                            stateDesc.getTtlConfig().getTimeToLive().toMillis(),
                            TimeUnit.MILLISECONDS);
                }

                return valueBytes;
            } catch (final NoSuchKeyException e) {
                // Object doesn't exist
                LOG.debug("No object found in S3 for key: {}", s3Key);
                return null;
            }
        } catch (final Exception e) {
            // Report error metrics
            metrics.reportError();

            LOG.error(
                    "Error reading state '{}' for key '{}' and namespace '{}' from S3",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            throw new RuntimeException("Error reading value from S3", e);
        }
    }

    /**
     * Inserts or updates a state entry in S3.
     *
     * <p>This method stores a specific state entry identified by the state name,
     * current key, and namespace in S3. If an entry with the same identifier already exists,
     * it is overwritten.
     *
     * <p>The method handles compression by automatically compressing the data
     * using Snappy compression before storing it in S3.
     *
     * <p>If the state has TTL enabled, it also registers a TTL timeout for the state entry.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param currentNamespace The namespace of the state to insert
     * @param serializedValue The serialized state value to store
     * @param <N> The type of the namespace
     */
    @Override
    public <N> void insertState(
            final StateDescriptor<?, ?> stateDesc,
            final N currentNamespace,
            final ByteBuffer serializedValue) {
        LOG.debug(
                "Inserting state '{}' for key '{}' and namespace '{}' to S3",
                stateDesc.getName(),
                getCurrentKey(),
                currentNamespace);

        try {
            // Get the S3 key
            final String s3Key = getS3Key(getCurrentKey(), currentNamespace, stateDesc);
            LOG.trace("S3 key created for insert: {}", s3Key);

            // Compress if in the configuration
            byte[] bucketValue = serializedValue.array();
            final long compressDurationMs;
            LOG.trace(
                    "Compressing state value using Snappy, original size: {}",
                    bucketValue.length);
            final long startCompress = System.nanoTime();
            final byte[] compressed = Snappy.compress(serializedValue.array());
            compressDurationMs = (System.nanoTime() - startCompress) / 1_000_000;

            // Report compression metrics
            metrics.reportCompression(compressDurationMs);

            LOG.trace(
                    "Value compressed successfully in {} ms, compressed size: {}, compression ratio: {}",
                    compressDurationMs,
                    compressed.length,
                    String.format("%.2f", (double) compressed.length / bucketValue.length));
            bucketValue = compressed;

            // Put the value
            LOG.trace(
                    "Executing PutObject request to S3 bucket: {} with key: {}",
                    awsModel.name(),
                    s3Key);

            // Start timing the write operation
            final long startTime = System.nanoTime();

            awsClient.putObject(
                    PutObjectRequest.builder().bucket(awsModel.name()).key(s3Key).build(),
                    RequestBody.fromBytes(bucketValue));

            // Calculate write duration in milliseconds
            final long writeDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            // Report write metrics
            metrics.reportWrite(bucketValue.length, writeDurationMs);

            LOG.info(
                    "S3 write operation completed in {} ms, object size: {} bytes, key: {}, compression time: {} ms",
                    writeDurationMs,
                    bucketValue.length,
                    s3Key,
                    compressDurationMs);

            LOG.debug("State '{}' inserted successfully to S3", stateDesc.getName());

            if (stateDesc.getTtlConfig().isEnabled()) {
                timerService.registerTimeout(
                        Tuple3.of(getCurrentKey(), currentNamespace,
                                stateDesc.getName()),
                        stateDesc.getTtlConfig().getTimeToLive().toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        } catch (final Exception e) {
            // Report error metrics
            metrics.reportError();

            LOG.error(
                    "Error writing state '{}' for key '{}' and namespace '{}' to S3",
                    stateDesc.getName(),
                    getCurrentKey(),
                    currentNamespace,
                    e);
            throw new RuntimeException("Error writing value to S3", e);
        }
    }

    /**
     * Creates an iterator over all state entries for a specific state descriptor.
     *
     * <p>This method returns an iterator that can be used to traverse all state entries
     * for a given state descriptor. It uses S3's list objects API to find all objects
     * with keys that match the state descriptor and current key.
     *
     * <p>The iterator handles pagination of S3 list results and deserializes the state entries
     * on demand as they are accessed.
     *
     * @param stateDesc The state descriptor containing the state name and configuration
     * @param <N> The type of the namespace
     * @param <SV> The type of the state value
     * @return An iterator over all state entries for the specified state
     * @throws IOException If an error occurs during iterator creation
     */

    @Override
    public <N, SV> AwsStateIterator<K, N, SV, ListObjectsV2Response> stateIterator(
            final StateDescriptor<?, ?> stateDesc) throws IOException {
        LOG.debug("Creating state iterator for state '{}' from S3", stateDesc.getName());

        final RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo = (RegisteredKeyValueStateBackendMetaInfo<N, SV>) this.kvStateInformation.get(
                stateDesc.getName());

        final DataOutputSerializer keyOutput = new DataOutputSerializer(64);
        keySerializer.serialize(getCurrentKey(), keyOutput);

        final String s3Key = S3KeyAdapter.createS3Key(
                jobInfo,
                taskInfo,
                keyOutput.getCopyOfBuffer(),
                null,
                stateDesc.getName(),
                configuration);

        LOG.trace("Listing objects in S3 bucket: {} with prefix: {}", awsModel.name(), s3Key);

        return new AwsStateIterator<>(
                awsClient.listObjectsV2Paginator(ListObjectsV2Request
                        .builder()
                        .bucket(awsModel.name())
                        .prefix(s3Key)
                        .build()), response -> {
            LOG.trace("Processing S3 list response with {} objects", response.contents().size());
            return response.contents().stream().map(obj -> {
                try {
                    LOG.trace("Processing S3 object with key: {}", obj.key());
                    final byte[] serializedKey = S3KeyAdapter.decodeSerializedKey(obj.key());
                    final byte[] serializedNamespace = S3KeyAdapter.decodeSerializedNamespace(obj.key());

                    final K key = keySerializer.deserialize(new DataInputDeserializer(serializedKey));
                    final N namespace = metaInfo
                            .getNamespaceSerializer()
                            .deserialize((new DataInputDeserializer(serializedNamespace)));

                    LOG.trace("Deserialized key: {} and namespace: {}", key, namespace);

                    final byte[] valueBytes = queryState(
                            stateDesc,
                            S3KeyAdapter.decodeSerializedNamespace(obj.key()));
                    if (valueBytes == null) {
                        LOG.warn("No value found for key: {} and namespace: {}", key, namespace);
                        throw new IOException("No value found for key and namespace");
                    }

                    LOG.trace("Deserializing state value");
                    final SV value = metaInfo
                            .getStateSerializer()
                            .deserialize(new DataInputDeserializer(valueBytes));

                    return new StateEntry.SimpleStateEntry<>(key, namespace, value);
                } catch (final IOException e) {
                    LOG.error("Error processing S3 object with key: {}", obj.key(), e);
                    throw new RuntimeException("Error processing S3 object", e);
                }
            }).iterator();
        });
    }

    /**
     * Handles timeout notifications for TTL-enabled state entries.
     *
     * <p>This method is called when a TTL timeout expires for a state entry. It checks
     * if the state entry has actually expired by comparing its last modified timestamp
     * with the TTL configuration. If the state has expired, it deletes the entry from S3.
     *
     * @param key A tuple containing the state key, namespace, and state name
     * @param ticket The UUID ticket for the timeout
     */

    @Override
    @SuppressWarnings("rawtypes")
    public void notifyTimeout(
            final Tuple3<K, Object, String> key,
            final UUID ticket) {
        if (this.timerService.isValid(key, ticket)) {
            LOG.info("Timer expired for key: {}", key);

            // Check the last modified timestamp against the state descriptor
            final StateDescriptor<?, ?> stateDesc = registeredStateDescriptors.get(key.f2);
            try {
                // Get the S3 key
                final String s3Key = this.getS3Key(key.f0, key.f1, stateDesc);

                final HeadObjectResponse response = awsClient.headObject(b -> b
                        .bucket(awsModel.name())
                        .key(s3Key));

                if (response
                        .lastModified()
                        .plusMillis(stateDesc.getTtlConfig().getTimeToLive().toMillis())
                        .isBefore(Instant.ofEpochMilli(this.ttlTimeProvider.currentTimestamp()))) {
                    LOG.info("State is expired, deleting from S3");
                    awsClient.deleteObject(b -> b.bucket(awsModel.name()).key(s3Key));
                    registeredStateDescriptors.remove(key.f2);
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <N> String getS3Key(
            final K currentKey,
            final N currentNamespace,
            final StateDescriptor<?, ?> stateDesc) throws IOException {
        // Get the S3 key
        final DataOutputSerializer keyOutput = new DataOutputSerializer(64);
        keySerializer.serialize(currentKey, keyOutput);

        final DataOutputSerializer namespaceOutput = new DataOutputSerializer(64);
        ((TypeSerializer<N>) kvStateInformation
                .get(stateDesc.getName())
                .getNamespaceSerializer()).serialize(currentNamespace, namespaceOutput);

        final String s3Key = S3KeyAdapter.createS3Key(
                jobInfo,
                taskInfo,
                keyOutput.getCopyOfBuffer(),
                namespaceOutput.getCopyOfBuffer(),
                stateDesc.getName(),
                configuration);

        LOG.trace("S3 key created for query: {}", s3Key);
        return s3Key;
    }

}
