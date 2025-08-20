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

package org.apache.flink.state.backend.aws.state.internal;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract base class for AWS-based state implementations.
 *
 * <p>This class provides common functionality for state implementations that use AWS services
 * for state storage. It handles serialization, deserialization, and key building.
 *
 * <p>Key features implemented in this base class:
 * <ul>
 *   <li>State value serialization and deserialization</li>
 *   <li>Key and namespace management</li>
 *   <li>State migration support</li>
 *   <li>Incremental state visitation</li>
 *   <li>Common state operations (clear, update, value retrieval)</li>
 * </ul>
 *
 * <p>Concrete implementations must provide service-specific functionality for state storage
 * and retrieval operations, including:
 * <ul>
 *   <li>State storage format and layout</li>
 *   <li>Service-specific optimizations</li>
 *   <li>Batch operations (if supported)</li>
 *   <li>Error handling and retries</li>
 * </ul>
 *
 * @param <K>  The type of the keys in the state backend.
 * @param <N>  The type of the namespace.
 * @param <SV> The type of the state value.
 */
public class AwsStateBase<K, N, SV> implements InternalKvState<K, N, SV> {

    private static final Logger LOG = LoggerFactory.getLogger(AwsStateBase.class);

    /**
     * The keyed state backend that owns this state.
     */
    protected final AwsKeyedStateBackendBase<K, ?, ?> backend;

    /**
     * The serializer for the keys.
     */
    protected final TypeSerializer<K> keySerializer;

    /**
     * The serializer for the namespace.
     */
    protected final TypeSerializer<N> namespaceSerializer;

    protected final StateDescriptor<?, SV> stateDescriptor;

    /**
     * The shared key builder.
     *
     * <p>This builder creates DynamoDB keys from Flink keys and namespaces.
     * It handles the serialization and formatting of keys for use in DynamoDB operations.
     */
    private final SerializedCompositeKeyBuilder<K> sharedKeyBuilder;

    /**
     * The current namespace.
     */
    protected N currentNamespace;

    /**
     * The output serializer for serializing objects.
     *
     * <p>This serializer is used to convert objects to binary format
     * for storage in DynamoDB.
     */
    protected final DataOutputSerializer outputSerializer;

    /**
     * The input deserializer for deserializing objects.
     *
     * <p>This deserializer is used to convert binary data from DynamoDB
     * back to objects.
     */
    protected final DataInputDeserializer inputDeserializer;

    /**
     * Creates a new AWS state.
     *
     * @param backend             The keyed state backend that owns this state.
     * @param keySerializer       The serializer for the keys.
     * @param namespaceSerializer The serializer for the namespace.
     */
    public AwsStateBase(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, SV> stateDescriptor) {
        this.backend = backend;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateDescriptor = stateDescriptor;
        this.sharedKeyBuilder = sharedKeyBuilder;
        this.outputSerializer = new DataOutputSerializer(128);
        this.inputDeserializer = new DataInputDeserializer();
    }


    /**
     * Clears the current state value from DynamoDB.
     *
     * <p>This method deletes the item in the DynamoDB table that corresponds to
     * the current key and namespace.
     */
    @Override
    public void clear() {
        try {
            backend.deleteState(stateDescriptor,
                    currentNamespace);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the current state value from DynamoDB.
     *
     * <p>This method fetches the item from the DynamoDB table that corresponds to
     * the current key and namespace, and deserializes the value.
     *
     * @return The deserialized state value, or null if no value exists.
     */
    public SV value() throws IOException {
        final byte[] valueBytes = backend.queryState(
                stateDescriptor,
                currentNamespace);
        if (valueBytes == null) {
            return null;
        }

        inputDeserializer.setBuffer(valueBytes);
        return stateDescriptor.getSerializer().deserialize(inputDeserializer);
    }

    /**
     * Updates the current state value in DynamoDB.
     *
     * <p>This method writes the serialized value to the DynamoDB table at the location
     * corresponding to the current key and namespace. If the value is null, the item
     * is deleted instead.
     *
     * @param value The new state value to store.
     */
    public void update(final SV value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        backend.insertState(stateDescriptor,
                currentNamespace, serializeValue(value));
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<SV> getValueSerializer() {
        return stateDescriptor.getSerializer();
    }

    /**
     * Sets the current namespace for this state.
     *
     * @param namespace The namespace to set.
     */
    @Override
    public void setCurrentNamespace(final N namespace) {
        this.currentNamespace = namespace;
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<SV> safeValueSerializer) throws Exception {
        final Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer);

        final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
                keyAndNamespace.f0,
                backend.getNumberOfKeyGroups());

        final SerializedCompositeKeyBuilder<K> keyBuilder = new SerializedCompositeKeyBuilder<>(safeKeySerializer,
                backend.getKeyGroupPrefixBytes(),
                32);
        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);

        final byte[] key = keyBuilder.buildCompositeKeyNamespace(
                keyAndNamespace.f1,
                namespaceSerializer);
        return backend.queryState(stateDescriptor, currentNamespace);
    }

    @Override
    public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(final int recommendedMaxNumberOfReturnedRecords) {
        try {
            return new AwsStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the current namespace for this state.
     *
     * @return The current namespace.
     */
    public N getCurrentNamespace() {
        return currentNamespace;
    }

    /**
     * Migrates a serialized value from one serializer to another.
     *
     * <p>This method is used during state migration when the serializer for a state
     * has changed. It deserializes the value using the old serializer and then
     * reserializes it using the new serializer.
     *
     * @param serializedValueInput  The input containing the serialized value.
     * @param serializedValueOutput The output to write the migrated value to.
     * @param previousSerializer    The previous serializer used to deserialize the value.
     * @param currentSerializer     The current serializer used to reserialize the value.
     * @throws IOException If an error occurs during migration.
     */
    public void migrateSerializedValue(
            final DataInputDeserializer serializedValueInput,
            final DataOutputSerializer serializedValueOutput,
            final TypeSerializer<SV> previousSerializer,
            final TypeSerializer<SV> currentSerializer) throws IOException {
        final SV value = previousSerializer.deserialize(serializedValueInput);
        currentSerializer.serialize(value, serializedValueOutput);
    }

    /**
     * Serializes a value to a DynamoDB attribute.
     *
     * <p>This method converts a state value to a binary DynamoDB attribute
     * using the value serializer.
     *
     * @param value The value to serialize.
     * @return The DynamoDB attribute containing the serialized value,
     * or null if the value is null.
     */
    protected ByteBuffer serializeValue(final SV value) {
        if (value == null) {
            return null;
        }

        try {
            outputSerializer.clear();
            stateDescriptor.getSerializer().serialize(value, outputSerializer);
            return ByteBuffer.wrap(outputSerializer.getCopyOfBuffer());
        } catch (final IOException e) {
            throw new RuntimeException("Error serializing value", e);
        }
    }

    /**
     * Implementation of the StateIncrementalVisitor interface for AWS state backends.
     */
    public class AwsStateIncrementalVisitor implements InternalKvState.StateIncrementalVisitor<K, N, SV> {

        private final Collection<StateEntry<K, N, SV>> initialEntries = new ArrayList<>();
        private final Iterator<StateEntry<K, N, SV>> iterator;

        public AwsStateIncrementalVisitor(final int recommendedMaxNumberOfReturnedRecords) throws IOException {

            this.iterator = backend.stateIterator(stateDescriptor.getName());

            for (int i = 0; i < recommendedMaxNumberOfReturnedRecords; i++) {
                initialEntries.add(this.iterator.next());
            }
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Collection<StateEntry<K, N, SV>> nextEntries() {
            return initialEntries;
        }

        @Override
        public void remove(final StateEntry<K, N, SV> stateEntry) {
            try {
                final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
                        stateEntry.getKey(),
                        backend.getNumberOfKeyGroups());

                final SerializedCompositeKeyBuilder<K> keyBuilder = new SerializedCompositeKeyBuilder<>(
                        keySerializer,
                        backend.getKeyGroupPrefixBytes(),
                        32);
                keyBuilder.setKeyAndKeyGroup(stateEntry.getKey(), keyGroup);
                backend.deleteState(
                        stateDescriptor,
                        currentNamespace);
            } catch (final IOException e) {
                throw new RuntimeException("Error removing state entry", e);
            }
        }

        @Override
        public void update(final StateEntry<K, N, SV> stateEntry, final SV newValue) {
            try {
                final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
                        stateEntry.getKey(),
                        backend.getNumberOfKeyGroups());

                final SerializedCompositeKeyBuilder<K> keyBuilder = new SerializedCompositeKeyBuilder<>(
                        keySerializer,
                        backend.getKeyGroupPrefixBytes(),
                        32);
                keyBuilder.setKeyAndKeyGroup(stateEntry.getKey(), keyGroup);

                backend.insertState(
                        stateDescriptor,
                        currentNamespace, serializeValue(newValue));
            } catch (final IOException e) {
                throw new RuntimeException("Error removing state entry", e);
            }
        }
    }
}
