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

package org.apache.flink.state.backend.aws.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.state.backend.aws.util.AwsStateUtil;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.core.SdkClient;

import javax.annotation.Nonnegative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link FullSnapshotResources} for AWS state backends.
 *
 * <p>This class provides the resources needed to create a full snapshot of the keyed state
* stored in AWS services like DynamoDB or S3. It implements the {@link FullSnapshotResources}
 * interface to integrate with Flink's checkpointing mechanism.
 *
 * <p>The snapshot resources include:
 * <ul>
 *   <li>State metadata snapshots for all registered states</li>
 *   <li>A {@link KeyValueStateIterator} implementation that iterates over all state entries</li>
 *   <li>Access to the key group range, key serializer, and compression decorator</li>
 * </ul>
 *
 * <p>This class is used during checkpoint and savepoint creation to capture the current state of the
 * AWS-backed keyed state backend. It handles the serialization and organization of state data
 * for efficient storage and retrieval.
 *
 * @param <K> The type of the key.
 * @param <C> The type of the AWS client (e.g., DynamoDbClient, S3Client).
 * @see FullSnapshotResources
 */
public class AwsFullSnapshotResources<K, C extends SdkClient> implements FullSnapshotResources<K> {

    /**
     * The keyed state backend that owns this state.
     */
    protected final AwsKeyedStateBackendBase<K, C, ?> backend;

    /**
     * Metadata snapshots for all registered states.
     * These snapshots contain information about the state types, names, and serializers.
     */
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

    /**
     * Resource lease to ensure proper resource management.
     * This lease is acquired from a ResourceGuard to prevent resource leaks.
     */
    private final ResourceGuard.Lease lease;

    /**
     * Metadata for all registered states.
     * This includes state information and snapshot transformers for each state.
     */
    private final List<RegisteredKeyValueStateBackendMetaInfo<?, ?>> metaData;

    /**
     * Number of bytes in the key-group prefix.
     * This is used for serializing and deserializing keys with their key group information.
     */
    @Nonnegative
    private final int keyGroupPrefixBytes;

    /**
     * The range of key groups covered by this state backend.
     * This defines which key groups are included in the snapshot.
     */
    private final KeyGroupRange keyGroupRange;

    /**
     * Serializer for the key type.
     * This is used to serialize and deserialize keys during snapshot creation.
     */
    private final TypeSerializer<K> keySerializer;

    /**
     * Decorator for stream compression.
     * This is used to compress state data during snapshot creation.
     */
    private final StreamCompressionDecorator streamCompressionDecorator;

    /**
     * Creates a new instance of DynamoDbFullSnapshotResources.
     *
     * @param backend
     * @param lease                      Resource lease to ensure proper resource management
     * @param metaDataCopy               Copy of metadata for all registered states
     * @param stateMetaInfoSnapshots     Metadata snapshots for all registered states
     * @param keyGroupPrefixBytes        Number of bytes in the key-group prefix
     * @param keyGroupRange              The range of key groups covered by this state backend
     * @param keySerializer              Serializer for the key type
     * @param streamCompressionDecorator Decorator for stream compression
     */
    public AwsFullSnapshotResources(
            final AwsKeyedStateBackendBase<K, C, ?> backend,
            final ResourceGuard.Lease lease,
            final List<RegisteredKeyValueStateBackendMetaInfo<?, ?>> metaDataCopy,
            final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            final int keyGroupPrefixBytes,
            final KeyGroupRange keyGroupRange,
            final TypeSerializer<K> keySerializer,
            final StreamCompressionDecorator streamCompressionDecorator) {
        this.backend = backend;
        this.lease = lease;
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.streamCompressionDecorator = streamCompressionDecorator;

        // we need to do this in the constructor, i.e. in the synchronous part of the snapshot
        this.metaData = metaDataCopy;
    }

    /**
     * Factory method to create a new instance of AwsFullSnapshotResources.
     *
     * @param kvStateInformation           Map of state name to state information
     * @param resourceGuard                Resource guard for managing resources
     * @param keyGroupRange                The range of key groups covered by this state backend
     * @param keySerializer                Serializer for the key type
     * @param keyGroupPrefixBytes          Number of bytes in the key-group prefix
     * @param keyGroupCompressionDecorator Decorator for key group compression
     * @param <K>                          The type of the key
     * @return A new instance of DynamoDbFullSnapshotResources
     * @throws IOException If an I/O error occurs
     */
    public static <K, C extends SdkClient> AwsFullSnapshotResources<K, C> create(
            final AwsKeyedStateBackendBase<K, C, ?> backend,
            final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation,
            final ResourceGuard resourceGuard,
            final KeyGroupRange keyGroupRange,
            final TypeSerializer<K> keySerializer,
            final int keyGroupPrefixBytes,
            final StreamCompressionDecorator keyGroupCompressionDecorator)
            throws IOException {

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
        final List<RegisteredKeyValueStateBackendMetaInfo<?, ?>> metaDataCopy = new ArrayList<>(kvStateInformation.size());

        for (final RegisteredKeyValueStateBackendMetaInfo<?, ?> stateInfo : kvStateInformation.values()) {
            // snapshot meta info
            stateMetaInfoSnapshots.add(stateInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }

        final ResourceGuard.Lease lease = resourceGuard.acquireResource();

        return new AwsFullSnapshotResources<>(
                backend,
                lease,
                metaDataCopy,
                stateMetaInfoSnapshots,
                keyGroupPrefixBytes,
                keyGroupRange,
                keySerializer, keyGroupCompressionDecorator);
    }

    /**
     * Creates a key-value state iterator for iterating over all state entries in DynamoDB.
     *
     * @return A key-value state iterator
     */
    @Override
    public KeyValueStateIterator createKVStateIterator() {
        return new KvStateIteratorAdapter(metaData.stream()
                .map(meta -> {
                    try {
                        return Tuple2.of(meta, backend.stateIterator(meta.getName()));
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(e -> e.f0, e -> e.f1)));
    }

    /**
     * Returns the metadata snapshots for all registered states.
     *
     * @return List of state metadata snapshots
     */
    @Override
    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    /**
     * Returns the range of key groups covered by this state backend.
     *
     * @return Key group range
     */
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    /**
     * Returns the serializer for the key type.
     *
     * @return Key serializer
     */
    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /**
     * Returns the decorator for stream compression.
     *
     * @return Stream compression decorator
     */
    @Override
    public StreamCompressionDecorator getStreamCompressionDecorator() {
        return streamCompressionDecorator;
    }

    /**
     * Releases the resources held by this object.
     */
    @Override
    public void release() {
        IOUtils.closeQuietly(lease);
    }

    /**
     * Adapter that implements the {@link KeyValueStateIterator} interface for iterating over
     * DynamoDB state entries.
     */
    private class KvStateIteratorAdapter<N, SV> implements KeyValueStateIterator {
        /**
         * Iterator over all state iterators.
         * This iterates over the metadata and state iterators for all registered states.
         */
        private final Iterator<Map.Entry<RegisteredKeyValueStateBackendMetaInfo<N, SV>, Iterator<? extends StateEntry<K, N, SV>>>> iterators;

        /**
         * Current state iterator.
         * This is the iterator for the current state being processed.
         */
        private Map.Entry<RegisteredKeyValueStateBackendMetaInfo<N, SV>, Iterator<? extends StateEntry<K, N, SV>>> currentIt;

        /**
         * Previous state entry.
         * This is used to determine if the key or key group has changed.
         */
        private StateEntry<K, N, SV> previous = null;

        /**
         * Current state entry.
         * This is the current DynamoDB item being processed.
         */
        private StateEntry<K,  N, SV> current = null;

        /**
         * Creates a new KvStateIteratorAdapter.
         *
         * @param iterators Map of metadata to state iterators
         */
        private KvStateIteratorAdapter(
                final Map<RegisteredKeyValueStateBackendMetaInfo<N, SV>,
                        Iterator<? extends StateEntry<K, N, SV>>> iterators) {
            this.iterators = iterators.entrySet().iterator();
        }

        /**
         * Advances to the next state entry.
         *
         * @throws IOException If an I/O error occurs
         */
        @Override
        public void next() throws IOException {
            previous = current;
            if (current == null || !currentIt.getValue().hasNext()) {
                if (iterators.hasNext()) {
                    currentIt = iterators.next();
                    current = currentIt.getValue().next();
                } else {
                    current = null;
                }
            } else {
                current = currentIt.getValue().next();
            }
        }

        /**
         * Returns the key group of the current state entry.
         *
         * @return Key group
         */
        @Override
        public int keyGroup() {
            try {
                final byte[] valueBytes = KvStateSerializer.serializeValue(
                        current.getState(),
                        currentIt.getKey().getStateSerializer());
                return AwsStateUtil.readKeyGroup(keyGroupPrefixBytes, valueBytes);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns the serialized key of the current state entry.
         *
         * @return Serialized key
         */
        @Override
        public byte[] key() {
            final DataOutputSerializer outputSerializer = new DataOutputSerializer(128);

            try {
                keySerializer.serialize(current.getKey(), outputSerializer);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return outputSerializer.getCopyOfBuffer();
        }

        /**
         * Returns the serialized value of the current state entry.
         *
         * @return Serialized value
         */
        @Override
        public byte[] value() {
            try {
                return KvStateSerializer.serializeValue(
                        current.getState(),
                        currentIt.getKey().getStateSerializer());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns the ID of the current key-value state.
         *
         * @return Key-value state ID
         */
        @Override
        public int kvStateId() {
            return 0;
        }

        /**
         * Returns whether the current state entry has a different key than the previous one.
         *
         * @return True if the key is different, false otherwise
         */
        @Override
        public boolean isNewKeyValueState() {
            try {
                final byte[] previousValueBytes = KvStateSerializer.serializeValue(
                        previous.getState(),
                        currentIt.getKey().getStateSerializer());
                return AwsStateUtil.readKey(keySerializer, currentIt.getKey().getNamespaceSerializer(), previousValueBytes) != key();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns whether the current state entry has a different key group than the previous one.
         *
         * @return True if the key group is different, false otherwise
         */
        @Override
        public boolean isNewKeyGroup() {
            try {
                final byte[] previousValueBytes = KvStateSerializer.serializeValue(
                        previous.getState(),
                        currentIt.getKey().getStateSerializer());
                return AwsStateUtil.readKeyGroup(keyGroupPrefixBytes, previousValueBytes) != keyGroup();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns whether the iterator is valid (i.e., has a current state entry).
         *
         * @return True if the iterator is valid, false otherwise
         */
        @Override
        public boolean isValid() {
            return current != null;
        }

        /**
         * Closes the iterator and releases its resources.
         */
        @Override
        public void close() {
            previous = null;
            current = null;
        }
    }
}
