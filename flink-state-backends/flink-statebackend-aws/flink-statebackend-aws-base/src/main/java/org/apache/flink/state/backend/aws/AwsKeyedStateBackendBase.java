/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.snapshot.AwsFullSnapshotResources;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.state.backend.aws.state.AwsStateIterator;
import org.apache.flink.state.backend.aws.state.internal.AwsAggregatingState;
import org.apache.flink.state.backend.aws.state.internal.AwsListState;
import org.apache.flink.state.backend.aws.state.internal.AwsMapState;
import org.apache.flink.state.backend.aws.state.internal.AwsReducingState;
import org.apache.flink.state.backend.aws.state.internal.AwsStateBase;
import org.apache.flink.state.backend.aws.state.internal.AwsValueState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPojo;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Abstract base class for AWS-based keyed state backends.
 *
 * <p>This class provides common functionality for keyed state backends that use AWS services
 * for state storage. It handles common operations like state registration, serialization,
 * and key management.
 *
 * <p>Key features implemented in this base class:
 * <ul>
 *   <li>State registration and management for all standard Flink state types</li>
 *   <li>Serialization and deserialization of state data</li>
 *   <li>Key context management and key group assignment</li>
 *   <li>Snapshot and checkpoint coordination</li>
 *   <li>State migration support for schema evolution</li>
 *   <li>Priority queue state handling</li>
 * </ul>
 *
 * <p>Concrete implementations must provide service-specific functionality for state storage
 * and retrieval operations, including:
 * <ul>
 *   <li>State insertion and querying</li>
 *   <li>State deletion</li>
 *   <li>State iteration</li>
 *   <li>Service-specific optimizations</li>
 * </ul>
 *
 * @param <K> The type of the keys in the state backend.
 * @param <C> The type of the AWS client (e.g., DynamoDbClient, S3Client).
 * @param <M> The type of the AWS model object (e.g., TableDescription, Bucket).
 */
@SuppressWarnings("unchecked")
public abstract class AwsKeyedStateBackendBase<K, C extends SdkClient, M extends SdkPojo>
        extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(AwsKeyedStateBackendBase.class);

    /** The metric group for this state backend. */
    protected final MetricGroup metricGroup;

    /** The AWS client for interacting with AWS services. */
    protected final C awsClient;

    /** The AWS model object containing service-specific configuration. */
    protected final M awsModel;

    /** Information about the current job, including job ID and other metadata. */
    protected final JobInfo jobInfo;

    /** Information about the current task, including task name and parallelism. */
    protected final TaskInfo taskInfo;

    /** The identifier for the operator that uses this state backend. */
    protected final String operatorIdentifier;

    /** The serializer for the keys. */
    protected final SerializedCompositeKeyBuilder<K> sharedKeyBuilder;

    /** Map of registered states by name. */
    protected final Map<String, State> registeredStates;

    /** Map of registered states descriptors by name. */
    protected final Map<String, StateDescriptor<?, ?>> registeredStateDescriptors;
    /**
     * Information about the k/v states, maintained in the order as we create them. This is used to
     * retrieve the column family that is used for a state and also for sanity checks when
     * restoring.
     *
     * <p>This map contains metadata about each registered state, including its name, namespace
     * serializer, state serializer, and other configuration details. The order of entries
     * is preserved to ensure consistent behavior during state restoration.
     */
    protected final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation;

    protected final AwsSnapshotStrategy<K, C, M> checkpointSnapshotStrategy;

    /** The number of Key-group-prefix bytes for the key. */
    @Nonnegative
    protected final int keyGroupPrefixBytes;

    private final HeapPriorityQueuesManager heapPriorityQueuesManager;

    /** Factory for priority queue state. */
    private final PriorityQueueSetFactory priorityQueueFactory;

    /**
     * Flag to determine whether to use snapshot compression or not.
     */
    protected final boolean useSnapshotCompression;

    /**
     * Creates a new AWS keyed state backend.
     *
     * <p>This constructor initializes the AWS keyed state backend with all required components
     * for state management. It sets up the key context, serialized composite key builder,
     * and other components needed for state operations.
     *
     * @param awsClient The AWS client for state operations (e.g., DynamoDbClient, S3Client).
     * @param awsModel The AWS model object containing service-specific configuration.
     * @param jobInfo The information about the current job, including job ID and other metadata.
     * @param operatorIdentifier The identifier for the operator that uses this state backend.
     * @param kvStateRegistry The registry for key-value state used for queryable state.
     * @param keySerializer The serializer for the state keys.
     * @param userCodeClassLoader The class loader for user code to ensure proper class resolution.
     * @param executionConfig The execution configuration with serialization settings.
     * @param ttlTimeProvider The provider for time-to-live functionality in state.
     * @param latencyTrackingStateConfig The configuration for tracking state access latency.
     * @param cancelStreamRegistry The registry for streams that should be closed on cancellation.
     * @param sharedKeyBuilder The builder for serialized composite keys.
     * @param compressionDecorator The decorator for stream compression during state serialization.
     * @param priorityQueueFactory The factory for creating priority queue state.
     * @param keyContext The key context for managing key groups.
     * @param registeredPQStates The map of registered priority queue states.
     * @param kvStateInformation The map of registered key-value state information.
     * @param checkpointSnapshotStrategy The strategy for creating state snapshots during checkpoints.
     * @param keyGroupPrefixBytes The number of bytes used for key group prefixes.
     * @param useSnapshotCompression Flag indicating whether to use compression for state data.
     */
    public AwsKeyedStateBackendBase(
            final C awsClient,
            final M awsModel,
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
            final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final AwsSnapshotStrategy<K, C, M> checkpointSnapshotStrategy,
            final int keyGroupPrefixBytes,
            final boolean useSnapshotCompression,
            final MetricGroup metricGroup) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                compressionDecorator,
                keyContext);

        this.awsClient = Preconditions.checkNotNull(awsClient);
        this.jobInfo = Preconditions.checkNotNull(jobInfo);
        this.taskInfo = Preconditions.checkNotNull(taskInfo);
        this.operatorIdentifier = operatorIdentifier;
        this.sharedKeyBuilder = Preconditions.checkNotNull(sharedKeyBuilder);
        this.awsModel = Preconditions.checkNotNull(awsModel);
        this.kvStateInformation = Preconditions.checkNotNull(kvStateInformation);
        this.checkpointSnapshotStrategy = Preconditions.checkNotNull(checkpointSnapshotStrategy);
        this.metricGroup = Preconditions.checkNotNull(metricGroup);
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.priorityQueueFactory = priorityQueueFactory;
        this.useSnapshotCompression = useSnapshotCompression;
        if (priorityQueueFactory instanceof HeapPriorityQueueSetFactory) {
            this.heapPriorityQueuesManager =
                    new HeapPriorityQueuesManager(
                            registeredPQStates,
                            (HeapPriorityQueueSetFactory) priorityQueueFactory,
                            keyContext.getKeyGroupRange(),
                            keyContext.getNumberOfKeyGroups());
        } else {
            this.heapPriorityQueuesManager = null;
        }

        this.registeredStates = new HashMap<>();
        this.registeredStateDescriptors = new HashMap<>();
    }

    @Override
    public <N> Stream<K> getKeys(final String state, final N namespace) {
        try {
            final AwsStateIterator<K, N, ?, ?> iterator = stateIterator(state);
            return iterator
                    .stream()
                    .map(StateEntry::getKey)
                    .distinct();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(final String state) {
        try {
            final AwsStateIterator<K, N, ?, ?> iterator = stateIterator(state);

        return iterator.stream()
                .map(entry -> (Tuple2<K, N>) Tuple2.of(entry.getKey(), entry.getNamespace()))
                .distinct();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Disposes of all resources held by this state backend.
     */
    @Override
    public void dispose() {
        LOG.debug("Disposing AWS keyed state backend for operator: {}", operatorIdentifier);
        registeredStates.clear();
        kvStateInformation.clear();
        LOG.debug("AWS keyed state backend disposed for operator: {}", operatorIdentifier);
    }

    @Override
    public void close() {
        LOG.info("Closing AWS keyed state backend for operator: {}", operatorIdentifier);
        dispose();
        if (awsClient != null) {
            try {
                awsClient.close();
                LOG.debug("AWS client closed successfully");
            } catch (final Exception e) {
                LOG.warn("Error closing AWS client", e);
            }
        }
        LOG.info("AWS keyed state backend closed for operator: {}", operatorIdentifier);
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull final CheckpointOptions checkpointOptions) throws Exception {
        LOG.info("Starting snapshot for checkpoint {} at {} for operator: {}",
                checkpointId, timestamp, operatorIdentifier);

        // flush everything into db before taking a snapshot
//        awsClient.flushNow();

        try {
            final RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture = new SnapshotStrategyRunner<>(
                    checkpointSnapshotStrategy.getDescription(),
                    checkpointSnapshotStrategy,
                    cancelStreamRegistry,
                    ASYNCHRONOUS).snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

            LOG.debug("Snapshot future created successfully for checkpoint {}", checkpointId);
            return snapshotFuture;
        } catch (final Exception e) {
            LOG.error("Failed to create snapshot for checkpoint {} for operator: {}",
                    checkpointId, operatorIdentifier, e);
            throw e;
        }
    }

    @Override
    public void notifyCheckpointComplete(final long completedCheckpointId) throws Exception {
        LOG.info("Notifying checkpoint complete for checkpoint {} for operator: {}",
                completedCheckpointId, operatorIdentifier);
        try {
            if (checkpointSnapshotStrategy != null) {
                checkpointSnapshotStrategy.notifyCheckpointComplete(completedCheckpointId);
                LOG.debug("Checkpoint {} completed successfully for operator: {}",
                        completedCheckpointId, operatorIdentifier);
            } else {
                LOG.warn("Checkpoint snapshot strategy is null, cannot notify checkpoint complete");
            }
        } catch (final Exception e) {
            LOG.error("Error notifying checkpoint complete for checkpoint {} for operator: {}",
                    completedCheckpointId, operatorIdentifier, e);
            throw e;
        }
    }

    @Override
    public void notifyCheckpointAborted(final long checkpointId) throws Exception {
        LOG.info("Notifying checkpoint aborted for checkpoint {} for operator: {}",
                checkpointId, operatorIdentifier);
        try {
            if (checkpointSnapshotStrategy != null) {
                checkpointSnapshotStrategy.notifyCheckpointAborted(checkpointId);
                LOG.debug("Checkpoint {} aborted successfully for operator: {}",
                        checkpointId, operatorIdentifier);
            } else {
                LOG.warn("Checkpoint snapshot strategy is null, cannot notify checkpoint aborted");
            }
        } catch (final Exception e) {
            LOG.error("Error notifying checkpoint aborted for checkpoint {} for operator: {}",
                    checkpointId, operatorIdentifier, e);
            throw e;
        }
    }

    /**
     * Creates a savepoint for this state backend.
     *
     * <p>This method creates a savepoint by capturing the current state of the backend.
     * It creates snapshot resources that can be used to restore the state later.
     * The savepoint operation is performed asynchronously to minimize impact on processing.
     *
     * <p>Savepoints differ from regular checkpoints in that they are explicitly triggered by users
     * and are not automatically cleaned up by Flink.
     *
     * @return SavepointResources containing the snapshot resources for this state backend.
     * @throws Exception If an error occurs during savepoint creation.
     */
    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        LOG.info("Creating savepoint for operator: {}", operatorIdentifier);

        // flush everything into db before taking a snapshot
//        dynamoDbClient.flushNow();

        try {
            final AwsFullSnapshotResources<K, C> snapshotResources = AwsFullSnapshotResources.create(
                    this,
                    kvStateInformation,
                    new ResourceGuard(),
                    keyGroupRange,
                    keySerializer,
                    sharedKeyBuilder.hashCode(),
                    keyGroupCompressionDecorator);

            LOG.debug("Savepoint snapshot resources created successfully for operator: {}", operatorIdentifier);
            return new SavepointResources<>(snapshotResources, ASYNCHRONOUS);
        } catch (final Exception e) {
            LOG.error("Failed to create savepoint for operator: {}", operatorIdentifier, e);
            throw e;
        }
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
    KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull final String stateName,
            @Nonnull final TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> HeapPriorityQueueSet<T> create(
            @Nonnull final String stateName,
            @Nonnull final TypeSerializer<T> byteOrderedElementSerializer,
            final boolean allowFutureMetadataUpdates) {
        if (this.heapPriorityQueuesManager != null) {
            return (HeapPriorityQueueSet<T>) this.heapPriorityQueuesManager.createOrUpdate(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        } else {
            return (HeapPriorityQueueSet<T>) priorityQueueFactory.create(
                    stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        }
    }

    /**
     * Creates or updates an internal state with the given namespace serializer and state descriptor.
     *
     * <p>This method is used internally by the state backend to create or update state objects.
     * It registers the state metadata and creates the appropriate state implementation based on
     * the state descriptor type.
     *
     * @param namespaceSerializer      The serializer for the namespace.
     * @param stateDesc                The state descriptor.
     * @param snapshotTransformFactory The factory for creating state snapshot transformers.
     * @param <N>                      The type of the namespace.
     * @param <SV>                     The type of the state value.
     * @param <SEV>                    The type of the transformed state value.
     * @param <S>                      The type of the state.
     * @param <IS>                     The type of the internal state.
     * @return The created or updated internal state.
     * @throws Exception if the state cannot be created or updated.
     */
    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull final TypeSerializer<N> namespaceSerializer,
            @Nonnull final StateDescriptor<S, SV> stateDesc,
            @Nonnull final StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
        return createOrUpdateInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull final TypeSerializer<N> namespaceSerializer,
            @Nonnull final StateDescriptor<S, SV> stateDesc,
            @Nonnull final StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            final boolean allowFutureMetadataUpdates) throws Exception {
        LOG.debug("Creating or updating internal state '{}' of type {} for operator: {}",
                stateDesc.getName(), stateDesc.getType(), operatorIdentifier);

        try {
            final RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult = tryRegisterKvStateInformation(
                    stateDesc,
                    namespaceSerializer,
                    snapshotTransformFactory,
                    allowFutureMetadataUpdates);

            LOG.trace("State '{}' registered successfully", stateDesc.getName());

    //        if (!allowFutureMetadataUpdates) {
    //        // Config compact filter only when no future metadata updates
    //            ttlCompactFiltersManager.configCompactFilter(
    //                    stateDesc, registerResult.getStateSerializer());
    //        }

            registeredStateDescriptors.put(stateDesc.getName(), stateDesc);
            final IS state = (IS) this.registeredStates.computeIfAbsent(
                    stateDesc.getName(),
                    e -> createInternalState(stateDesc, registerResult));

            LOG.debug("State '{}' created or updated successfully for operator: {}",
                    stateDesc.getName(), operatorIdentifier);
            return state;
        } catch (final Exception e) {
            LOG.error("Failed to create or update state '{}' for operator: {}",
                    stateDesc.getName(), operatorIdentifier, e);
            throw e;
        }
    }

    /**
     * Creates an internal state object based on the state descriptor type.
     *
     * <p>This method instantiates the appropriate DynamoDB state implementation based on the
     * type of state descriptor provided (ValueState, ListState, MapState, etc.).
     *
     * @param stateDesc      The state descriptor.
     * @param registerResult The registered metadata for the state.
     * @param <N>            The type of the namespace.
     * @param <SV>           The type of the state value.
     * @param <S>            The type of the state.
     * @param <IS>           The type of the internal state.
     * @return The created internal state.
     */
    @Nonnull
    public final <N, SV, S extends State, IS extends S> IS createInternalState(
            @Nonnull final StateDescriptor<S, SV> stateDesc,
            final RegisteredKeyValueStateBackendMetaInfo<N, SV> registerResult) {
        LOG.debug("Creating internal state '{}' of type {} for operator: {}",
                stateDesc.getName(), stateDesc.getClass().getSimpleName(), operatorIdentifier);

        // Create a new state based on the state descriptor type
        final AwsStateBase<K, N, ?> newState;

        try {
            if (stateDesc instanceof ValueStateDescriptor) {
                LOG.trace("Creating ValueState for '{}'", stateDesc.getName());
                newState = new AwsValueState<>(
                        this,
                        keySerializer,
                        registerResult.getNamespaceSerializer(),
                        sharedKeyBuilder,
                        stateDesc);
            } else if (stateDesc instanceof ListStateDescriptor) {
                LOG.trace("Creating ListState for '{}'", stateDesc.getName());
                newState = new AwsListState<>(
                        this,
                        keySerializer,
                        registerResult.getNamespaceSerializer(),
                        sharedKeyBuilder,
                        (ListStateDescriptor<SV>) stateDesc);
            } else if (stateDesc instanceof MapStateDescriptor) {
                LOG.trace("Creating MapState for '{}'", stateDesc.getName());
                newState = new AwsMapState<>(
                        this,
                        keySerializer,
                        registerResult.getNamespaceSerializer(),
                        sharedKeyBuilder,
                        (MapStateDescriptor<?, ?>) stateDesc);
            } else if (stateDesc instanceof ReducingStateDescriptor) {
                LOG.trace("Creating ReducingState for '{}'", stateDesc.getName());
                newState = new AwsReducingState<>(
                        this,
                        keySerializer,
                        registerResult.getNamespaceSerializer(),
                        sharedKeyBuilder,
                        stateDesc,
                        ((ReducingStateDescriptor<SV>) stateDesc).getReduceFunction()
                );
            } else if (stateDesc instanceof AggregatingStateDescriptor) {
                LOG.trace("Creating AggregatingState for '{}'", stateDesc.getName());
                newState = new AwsAggregatingState<>(
                        this,
                        keySerializer,
                        registerResult.getNamespaceSerializer(),
                        sharedKeyBuilder,
                        stateDesc,
                        ((AggregatingStateDescriptor<?, SV, ?>) stateDesc).getAggregateFunction());
            } else {
                LOG.error("Unsupported state descriptor type: {}", stateDesc.getClass());
                throw new UnsupportedOperationException("State descriptor type " + stateDesc.getClass()
                        + " is not supported by AWS state backend");
            }

            LOG.debug("Internal state '{}' created successfully for operator: {}",
                    stateDesc.getName(), operatorIdentifier);
            return (IS) newState;
        } catch (final Exception e) {
            LOG.error("Failed to create internal state '{}' for operator: {}",
                    stateDesc.getName(), operatorIdentifier, e);
            throw e;
        }
    }

    /**
     * Registers a k/v state information, which includes its state id, type, RocksDB column family
     * handle, and serializers.
     *
     * <p>When restoring from a snapshot, we don’t restore the individual k/v states, just the
     * global RocksDB database and the list of k/v state information. When a k/v state is first
     * requested we check here whether we already have a registered entry for that and return it
     * (after some necessary state compatibility checks) or create a new one if it does not exist.
     */
    private <N, S extends State, SV, SEV> RegisteredKeyValueStateBackendMetaInfo<N, SV> tryRegisterKvStateInformation(
            final StateDescriptor<S, SV> stateDesc,
            final TypeSerializer<N> namespaceSerializer,
            @Nonnull final StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            final boolean allowFutureMetadataUpdates) throws Exception {

        final RegisteredKeyValueStateBackendMetaInfo<K, N> oldStateInfo = (RegisteredKeyValueStateBackendMetaInfo<K, N>) kvStateInformation.get(stateDesc.getName());

        final TypeSerializer<SV> stateSerializer = stateDesc.getSerializer();

        RegisteredKeyValueStateBackendMetaInfo<N, SV> newMetaInfo;
        if (oldStateInfo != null) {
            final RegisteredKeyValueStateBackendMetaInfo<N, SV> castedMetaInfo = (RegisteredKeyValueStateBackendMetaInfo<N, SV>) oldStateInfo;

            newMetaInfo = updateRestoredStateMetaInfo(
                    castedMetaInfo,
                    stateDesc,
                    namespaceSerializer,
                    stateSerializer);

        } else {
            newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
                    stateDesc.getType(),
                    stateDesc.getName(),
                    namespaceSerializer,
                    stateSerializer,
                    StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        }
        newMetaInfo = allowFutureMetadataUpdates ? newMetaInfo.withSerializerUpgradesAllowed() : newMetaInfo;
        kvStateInformation.put(stateDesc.getName(), newMetaInfo);

//        final StateSnapshotTransformer.StateSnapshotTransformFactory<SV> wrappedSnapshotTransformFactory =
//                wrapStateSnapshotTransformFactory(
//                        stateDesc, snapshotTransformFactory, newMetaInfo.getStateSerializer());
//        newMetaInfo.updateSnapshotTransformFactory(wrappedSnapshotTransformFactory);

        return newMetaInfo;
    }

    private <N, S extends State, SV> RegisteredKeyValueStateBackendMetaInfo<N, SV> updateRestoredStateMetaInfo(
            final RegisteredKeyValueStateBackendMetaInfo<N, SV> oldStateInfo,
            final StateDescriptor<S, SV> stateDesc,
            final TypeSerializer<N> namespaceSerializer,
            final TypeSerializer<SV> stateSerializer) throws Exception {

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        final TypeSerializer<N> previousNamespaceSerializer = oldStateInfo.getNamespaceSerializer();

        final TypeSerializerSchemaCompatibility<N> compatibility = oldStateInfo.updateNamespaceSerializer(
                namespaceSerializer);
        if (compatibility.isCompatibleAfterMigration() || compatibility.isIncompatible()) {
            throw new StateMigrationException("The new namespace serializer (" + namespaceSerializer
                    + ") must be compatible with the old namespace serializer ("
                    + previousNamespaceSerializer + ").");
        }

        oldStateInfo.checkStateMetaInfo(stateDesc);

        // fetch current serializer now because if it is incompatible, we can't access
        // it anymore to improve the error message
        final TypeSerializer<SV> previousStateSerializer = oldStateInfo.getStateSerializer();

        final TypeSerializerSchemaCompatibility<SV> newStateSerializerCompatibility = oldStateInfo.updateStateSerializer(
                stateSerializer);
        if (newStateSerializerCompatibility.isCompatibleAfterMigration()) {
            migrateStateValues(stateDesc, oldStateInfo);
        } else if (newStateSerializerCompatibility.isIncompatible()) {
            throw new StateMigrationException("The new state serializer (" + stateSerializer
                    + ") must not be incompatible with the old state serializer ("
                    + previousStateSerializer + ").");
        }

        return oldStateInfo;
    }

    /**
     * Migrate only the state value, that is the "value" that is stored in RocksDB. We don't migrate
     * the key here, which is made up of key group, key, namespace and map key (in case of
     * MapState).
     */
    @SuppressWarnings("unchecked")
    private <N, S extends State, SV> void migrateStateValues(
            final StateDescriptor<S, SV> stateDesc,
            final RegisteredKeyValueStateBackendMetaInfo<N, SV> stateMetaInfo) throws Exception {

        if (stateDesc.getType() == StateDescriptor.Type.MAP) {
            final TypeSerializerSnapshot<SV> previousSerializerSnapshot = stateMetaInfo.getPreviousStateSerializerSnapshot();
            checkState(
                    previousSerializerSnapshot != null,
                    "the previous serializer snapshot should exist.");
            checkState(
                    previousSerializerSnapshot instanceof MapSerializerSnapshot,
                    "previous serializer snapshot should be a MapSerializerSnapshot.");

            final TypeSerializer<SV> newSerializer = stateMetaInfo.getStateSerializer();
            checkState(
                    newSerializer instanceof MapSerializer,
                    "new serializer should be a MapSerializer.");

            final MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) newSerializer;
            final MapSerializerSnapshot<?, ?> mapSerializerSnapshot = (MapSerializerSnapshot<?, ?>) previousSerializerSnapshot;
            if (!checkMapStateKeySchemaCompatibility(mapSerializerSnapshot, mapSerializer)) {
                throw new StateMigrationException(
                        "The new serializer for a MapState requires state migration in order for the job to proceed, since the key schema has changed. However, migration for MapState currently only allows value schema evolutions.");
            }
        }

        LOG.info(
                "Performing state migration for state {} because the state serializer's schema, i.e. serialization format, has changed.",
                stateDesc);

        // we need to get an actual state instance because migration is different
        // for different state types. For example, ListState needs to deal with
        // individual elements
        final State state = createInternalState(stateDesc, stateMetaInfo);
        if (!(state instanceof AwsStateBase)) {
            throw new FlinkRuntimeException(
                    "State should be an AwsStateBase but is " + state);
        }

        @SuppressWarnings("unchecked") final AwsStateBase<K, N, SV> awsStateBase = (AwsStateBase<K, N, SV>) state;

        final Iterator<StateEntry<K, N, SV>> stateIterator = stateIterator(stateDesc);

        final DataInputDeserializer serializedValueInput = new DataInputDeserializer();
        final DataOutputSerializer migratedSerializedValueOutput = new DataOutputSerializer(512);
        while (stateIterator.hasNext()) {
            final StateEntry<K, N, SV> entry = stateIterator.next();

            final SerializedCompositeKeyBuilder<K> keyBuilder = new SerializedCompositeKeyBuilder<>(
                    getKeySerializer(),
                    keyGroupPrefixBytes,
                    512);

            final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
                    entry.getKey(),
                    getNumberOfKeyGroups());
            keyBuilder.setKeyAndKeyGroup(entry.getKey(), keyGroup);

            final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) kvStateInformation
                    .get(stateDesc.getName())
                    .getNamespaceSerializer();
            final byte[] serializedKeyAndNamespace = keyBuilder.buildCompositeKeyNamespace(
                    entry.getNamespace(),
                    namespaceSerializer);

            serializedValueInput.setBuffer(KvStateSerializer.serializeValue(entry.getState(), stateDesc.getSerializer()));

            awsStateBase.migrateSerializedValue(
                    serializedValueInput,
                    migratedSerializedValueOutput,
                    stateMetaInfo.getPreviousStateSerializer(),
                    stateMetaInfo.getStateSerializer());

            insertState(stateDesc,
                    entry.getNamespace(), ByteBuffer.wrap(migratedSerializedValueOutput.getCopyOfBuffer())
            );
            migratedSerializedValueOutput.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private static <UK> boolean checkMapStateKeySchemaCompatibility(
            final MapSerializerSnapshot<?, ?> mapStateSerializerSnapshot,
            final MapSerializer<?, ?> newMapStateSerializer) {
        final TypeSerializerSnapshot<UK> previousKeySerializerSnapshot = (TypeSerializerSnapshot<UK>) mapStateSerializerSnapshot.getKeySerializerSnapshot();
        final TypeSerializer<UK> newUserKeySerializer = (TypeSerializer<UK>) newMapStateSerializer.getKeySerializer();

        final TypeSerializerSchemaCompatibility<UK> keyCompatibility = newUserKeySerializer
                .snapshotConfiguration()
                .resolveSchemaCompatibility(previousKeySerializerSnapshot);
        return keyCompatibility.isCompatibleAsIs();
    }

    /**
     * Gets the number of bytes used for key group prefixes.
     *
     * @return The number of bytes used for key group prefixes.
     */
    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    @Override
    public int numKeyValueStateEntries() {
        return registeredStates.size();
    }

    @Override
    public void setCurrentKey(final K newKey) {
        super.setCurrentKey(newKey);
        this.sharedKeyBuilder.setKeyAndKeyGroup(getCurrentKey(), getCurrentKeyGroupIndex());
    }

    /**
     * Returns an iterator over all state entries for the specified state name.
     *
     * <p>This method provides access to all key-namespace pairs and their associated state values
     * for a given state name. The iterator allows traversing all state entries without loading
     * them all into memory at once.
     *
     * <p>Concrete implementations must provide the appropriate iterator implementation
     * based on the specific AWS service being used.
     *
     * @param state The name of the state to iterate over.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the state value.
     * @return An iterator over all state entries for the specified state.
     */
    public <N, SV> AwsStateIterator<K, N, SV, ?> stateIterator(final String state) throws IOException {
        return stateIterator(registeredStateDescriptors.get(state));
    }

    /**
     * Returns an iterator over all state entries for the specified state name.
     *
     * <p>This method provides access to all key-namespace pairs and their associated state values
     * for a given state name. The iterator allows traversing all state entries without loading
     * them all into memory at once.
     *
     * <p>Concrete implementations must provide the appropriate iterator implementation
     * based on the specific AWS service being used.
     *
     * @param stateDesc The name of the state to iterate over.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the state value.
     * @return An iterator over all state entries for the specified state.
     */
    public abstract <N, SV> AwsStateIterator<K, N, SV, ?> stateIterator(final StateDescriptor<?, ?> stateDesc) throws IOException;

    /**
     * Deletes a state entry from the backend.
     *
     * <p>This method removes a specific state entry identified by the state name and
     * serialized key-namespace pair from the AWS storage service.
     *
     * <p>Concrete implementations must provide the appropriate deletion logic
     * based on the specific AWS service being used.
     *
     * @param stateDesc The name of the state to delete.
     *
     * @throws IOException If an error occurs during the deletion operation.
     */
    public abstract <N> void deleteState(
            final StateDescriptor<?, ?> stateDesc,
            N currentNamespace) throws IOException;

    /**
     * Queries a state entry from the backend.
     *
     * <p>This method retrieves a specific state entry identified by the state name and
     * serialized key-namespace pair from the AWS storage service.
     *
     * <p>Concrete implementations must provide the appropriate query logic
     * based on the specific AWS service being used.
     *
     * @param stateDesc The name of the state to query.
     * @param currentNamespace
     *
     * @return The serialized state value, or null if the state entry does not exist.
     *
     * @throws IOException If an error occurs during the query operation.
     */
    public abstract <N> byte[] queryState(
            final StateDescriptor<?, ?> stateDesc,
            N currentNamespace) throws IOException;

    /**
     * Inserts or updates a state entry in the backend.
     *
     * <p>This method stores a specific state entry identified by the state name and
     * serialized key-namespace pair in the AWS storage service. If an entry with the
     * same identifier already exists, it is overwritten.
     *
     * <p>Concrete implementations must provide the appropriate insertion logic
     * based on the specific AWS service being used.
     *
     * @param <N> The type of the namespace.
     * @param stateDesc The name of the state to insert or update.
     * @param currentNamespace The current namespace object (used for additional context in some implementations).
     * @param serializedValue The serialized state value to store.
     *
     * @throws IOException If an error occurs during the insertion operation.
     */
    public abstract <N> void insertState(
            final StateDescriptor<?, ?> stateDesc,
            N currentNamespace, final ByteBuffer serializedValue) throws IOException;
}
