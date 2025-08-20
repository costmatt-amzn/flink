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

package org.apache.flink.state.backend.aws.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.util.IOUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPojo;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Abstract base class for AWS snapshot strategies.
 *
 * <p>This class provides common functionality for snapshot strategies that use AWS services
 * for state storage. It handles the creation of snapshot resources and manages the snapshot
 * lifecycle.
 *
 * <p>The snapshot strategy is responsible for:
 * <ul>
 *   <li>Preparing resources for snapshots</li>
 *   <li>Executing snapshot operations asynchronously</li>
 *   <li>Materializing state metadata</li>
 *   <li>Creating state handles that can be used for recovery</li>
 * </ul>
 *
 * <p>Concrete implementations must provide service-specific functionality for creating
 * snapshot resources and performing the actual snapshot operations, including:
 * <ul>
 *   <li>Persisting state data to the specific AWS service</li>
 *   <li>Managing service-specific resources during snapshot operations</li>
 *   <li>Implementing checkpoint completion and abortion handling</li>
 * </ul>
 *
 * @param <K> The type of the keys in the state backend.
 * @param <C> The type of the AWS client (e.g., DynamoDbClient, S3Client).
 * @param <M> The type of the AWS model object (e.g., TableDescription, Bucket).
 */
public abstract class AwsSnapshotStrategy<K, C extends SdkClient, M extends SdkPojo>
        implements SnapshotStrategy<KeyedStateHandle, AwsSnapshotStrategy.AwsSnapshotResources>,
        CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(AwsSnapshotStrategy.class);

    /** The metric group for this snapshot strategy. */
    protected final MetricGroup metricGroup;

    /** The AWS client for interacting with AWS services. */
    protected final C awsClient;

    protected final M awsModel;

    /** The unique identifier for this snapshot strategy. */
    protected final UUID backendUID;

    /** The key group range for this snapshot strategy. */
    protected final KeyGroupRange keyGroupRange;

    /** The configuration for local recovery. */
    protected final LocalRecoveryConfig localRecoveryConfig;

    /** Registry for temporary resources that need to be cleaned up. */
    @Nonnull
    protected final CloseableRegistry tmpResourcesRegistry;

    /** The serializer for the keys. */
    protected final TypeSerializer<K> keySerializer;

    /** Key/Value state meta info from the backend. */
    @Nonnull
    protected final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation;

    /**
     * Creates a new AWS snapshot strategy.
     *
     * @param awsClient The AWS client for state operations.
     * @param backendUID The unique identifier for this snapshot strategy.
     * @param keyGroupRange The key group range for this snapshot strategy.
     * @param localRecoveryConfig The configuration for local recovery.
     * @param keySerializer The serializer for the keys.
     */
    public AwsSnapshotStrategy(
            final C awsClient, M awsModel,
            final UUID backendUID,
            final KeyGroupRange keyGroupRange,
            final LocalRecoveryConfig localRecoveryConfig,
            final TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final MetricGroup metricGroup) {
        this.awsClient = awsClient;
        this.awsModel = awsModel;
        this.backendUID = backendUID;
        this.keyGroupRange = keyGroupRange;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keySerializer = keySerializer;
        this.kvStateInformation = kvStateInformation;
        this.tmpResourcesRegistry = new CloseableRegistry();
        this.metricGroup = metricGroup;
    }

    /**
     * Returns a description of this snapshot strategy.
     *
     * @return The table name as the description
     */
    @Nonnull
    public String getDescription() {
        return awsModel.toString();
    }

    @Override
    public AwsSnapshotResources syncPrepareResources(final long checkpointId) {
        final List<StateMetaInfoSnapshot> snapshots = new ArrayList<>(kvStateInformation.size());

        // snapshot meta data to save
        for (final Map.Entry<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> entry : kvStateInformation.entrySet()) {
            snapshots.add(entry.getValue().snapshot());
        }

        return new AwsSnapshotResources(snapshots);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            final AwsSnapshotResources syncPartResource,
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull final CheckpointOptions checkpointOptions) {
        if (syncPartResource.getStateMetaInfoSnapshots().isEmpty()) {
            return registry -> SnapshotResult.empty();
        }

        return new AwsSnapshotOperation(
                checkpointId,
                streamFactory,
                syncPartResource.getStateMetaInfoSnapshots());
    }

    /**
     * Materializes metadata for a snapshot.
     *
     * <p>This method serializes the state metadata and writes it to a checkpoint stream.
     * The resulting stream state handle is returned as part of the snapshot result.
     *
     * @param snapshotCloseableRegistry Registry for resources that should be closed when the snapshot completes
     * @param tmpResourcesRegistry Registry for temporary resources
     * @param stateMetaInfoSnapshots Metadata snapshots for all registered states
     * @param checkpointId The ID of the checkpoint
     * @param checkpointStreamFactory The factory for creating checkpoint streams
     * @return A snapshot result containing the stream state handle
     * @throws Exception If an error occurs during materialization
     */
    @Nonnull
    protected SnapshotResult<StreamStateHandle> materializeMetaData(
            @Nonnull final CloseableRegistry snapshotCloseableRegistry,
            @Nonnull final CloseableRegistry tmpResourcesRegistry,
            @Nonnull final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            final long checkpointId,
            @Nonnull final CheckpointStreamFactory checkpointStreamFactory) throws Exception {

        CheckpointStreamWithResultProvider streamWithResultProvider = localRecoveryConfig.isLocalRecoveryEnabled()
                ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                checkpointId,
                CheckpointedStateScope.EXCLUSIVE,
                checkpointStreamFactory,
                localRecoveryConfig
                        .getLocalStateDirectoryProvider()
                        .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled()))
                : CheckpointStreamWithResultProvider.createSimpleStream(
                CheckpointedStateScope.EXCLUSIVE,
                checkpointStreamFactory);

        snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

        try {
            final KeyedBackendSerializationProxy<K> serializationProxy = new KeyedBackendSerializationProxy<>(
                    keySerializer,
                    stateMetaInfoSnapshots,
                    true);

            final DataOutputView out = new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

            serializationProxy.write(out);

            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                final SnapshotResult<StreamStateHandle> result = streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                streamWithResultProvider = null;
                tmpResourcesRegistry.registerCloseable(
                        () -> StateUtil.discardStateObjectQuietly(result));
                return result;
            } else {
                throw new IOException("Stream already closed and cannot return a handle.");
            }
        } finally {
            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                IOUtils.closeQuietly(streamWithResultProvider);
            }
        }
    }

    public abstract long persist(final long checkpointId, byte[] serializedSnapshots);

    /**
     * Implementation of the SnapshotResources interface for AWS state backends.
     */
    public static class AwsSnapshotResources implements SnapshotResources {
        /** Metadata snapshots for all registered states. */
        private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        /**
         * Creates a new NativeDynamoDbSnapshotResources.
         *
         * @param stateMetaInfoSnapshots Metadata snapshots for all registered states
         */
        public AwsSnapshotResources(
                final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        /**
         * Releases the resources held by this object.
         *
         * <p>For DynamoDB, there are no resources to release, so this is a no-op.
         */
        @Override
        public void release() {
            // no-op
        }

        public List<StateMetaInfoSnapshot> getStateMetaInfoSnapshots() {
            return stateMetaInfoSnapshots;
        }
    }

    /**
     * Operation for creating a AWS snapshot.
     *
     * <p>This class implements the {@link SnapshotResultSupplier} interface to provide
     * an asynchronous snapshot operation for the AWS state backend.
     */
    public class AwsSnapshotOperation implements SnapshotResultSupplier<KeyedStateHandle> {

        /** The ID of the checkpoint. */
        private final long checkpointId;

        /** Stream factory that creates the output streams to DFS. */
        @Nonnull
        protected final CheckpointStreamFactory checkpointStreamFactory;

        /** The state meta data. */
        @Nonnull
        protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        /** Serializer for lists of byte arrays. */
        protected final ListDelimitedSerializer serializer = new ListDelimitedSerializer();

        /**
         * Creates a new AwsSnapshotOperation.
         *
         * @param checkpointId The ID of the checkpoint
         * @param checkpointStreamFactory The factory for creating checkpoint streams
         * @param stateMetaInfoSnapshots Metadata snapshots for all registered states
         */
        public AwsSnapshotOperation(
                final long checkpointId,
                @Nonnull final CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.checkpointId = checkpointId;
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        /**
         * Executes the snapshot operation.
         *
         * <p>This method serializes the state metadata, stores it in DynamoDB, and creates
         * a state handle that can be used to restore the state.
         *
         * @param snapshotCloseableRegistry Registry for resources that should be closed when the snapshot completes
         * @return A snapshot result containing the keyed state handle
         * @throws Exception If an error occurs during the snapshot operation
         */
        @Override
        public SnapshotResult<KeyedStateHandle> get(final CloseableRegistry snapshotCloseableRegistry) throws Exception {
            final List<byte[]> serializedSnapshots = new ArrayList<>(stateMetaInfoSnapshots.size());
            for (final StateMetaInfoSnapshot snapshot : stateMetaInfoSnapshots) {
                try (final ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
                    StateMetaInfoSnapshotReadersWriters
                            .getWriter()
                            .writeStateMetaInfoSnapshot(
                                    snapshot,
                                    new DataOutputViewStreamWrapper(out));

                    serializedSnapshots.add(out.toByteArray());
                }
            }

            // Handles to all the files in the current snapshot will go here
            final List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateFiles = new ArrayList<>();

            final byte[] serializedList = serializer.serializeList(serializedSnapshots,
                    BytePrimitiveArraySerializer.INSTANCE);
            final long persisted = persist(checkpointId, serializedList);

            final SnapshotResult<StreamStateHandle> metaStateHandle = materializeMetaData(
                    snapshotCloseableRegistry,
                    tmpResourcesRegistry,
                    stateMetaInfoSnapshots,
                    checkpointId,
                    checkpointStreamFactory);
            long checkpointedSize = metaStateHandle.getStateSize();
            checkpointedSize += persisted * FileUtils.ONE_GB;

            return SnapshotResult.of(new IncrementalRemoteKeyedStateHandle(
                    backendUID,
                    keyGroupRange,
                    checkpointId,
                    Collections.emptyList(),
                    privateFiles,
                    metaStateHandle.getJobManagerOwnedSnapshot(),
                    checkpointedSize));
        }
    }
}
