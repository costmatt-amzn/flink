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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * AWS implementation of {@link MapState}.
 *
 * <p>This class provides an AWS-backed implementation of Flink's {@link MapState} interface,
 * which stores a map of key-value pairs associated with a key and namespace. The map is stored
* as a serialized byte array in an AWS service (like DynamoDB or S3) using the composite key
 * structure defined in the AWS state backend.
 *
 * <p>The state is accessed and modified using the standard {@link MapState} methods:
 * <ul>
 *   <li>{@link #get(Object)} - Retrieves a value by key from the AWS service</li>
 *   <li>{@link #put(Object, Object)} - Adds or updates a key-value pair</li>
 *   <li>{@link #putAll(Map)} - Adds or updates multiple key-value pairs</li>
 *   <li>{@link #remove(Object)} - Removes a key-value pair</li>
 *   <li>{@link #contains(Object)} - Checks if a key exists</li>
 *   <li>{@link #entries()} - Returns all key-value pairs</li>
 *   <li>{@link #keys()} - Returns all keys</li>
 *   <li>{@link #values()} - Returns all values</li>
 *   <li>{@link #iterator()} - Returns an iterator over all key-value pairs</li>
 *   <li>{@link #isEmpty()} - Checks if the map is empty</li>
 *   <li>{@link #clear()} - Removes all key-value pairs from the AWS service</li>
 * </ul>
 *
 * <p>This implementation extends {@link AwsStateBase} to leverage common functionality
 * for AWS state operations, including serialization, deserialization, and key building.
 * The actual storage operations are delegated to the specific AWS service implementation
 * in the backend.
 *
 * <p>Performance considerations:
 * <ul>
 *   <li>Each operation requires a network call to the AWS service</li>
 *   <li>The entire map is serialized and stored as a single value</li>
 *   <li>For large maps, consider using batching when possible</li>
 *   <li>Operations like {@link #get(Object)} and {@link #contains(Object)} require loading the entire map</li>
 * </ul>
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the user keys in the map.
 * @param <UV> The type of the user values in the map.
 *
 * @see MapState
 * @see AwsStateBase
 */
public final class AwsMapState<K, N, UK, UV>
        extends AwsStateBase<K, N, Map<UK, UV>>
        implements MapState<UK, UV> {

    /**
     * Creates a new AWS-backed MapState.
     *
     * <p>This constructor initializes a MapState implementation that stores its data in an AWS service.
     * It sets up the necessary serializers and key builders for state operations.
     *
     * @param backend The AWS keyed state backend that manages this state
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param sharedKeyBuilder The shared key builder for constructing composite keys
     * @param stateDescriptor The state descriptor containing configuration for this state
     */
    public AwsMapState(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, Map<UK, UV>> stateDescriptor) {
        super(
                backend,
                keySerializer,
                namespaceSerializer,
                sharedKeyBuilder,
                stateDescriptor
        );
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param key The key to look up
     * @return The value associated with the key, or null if the key does not exist
     * @throws NullPointerException if the key is null
     */
    @Override
    public UV get(final UK key) throws IOException {
        Preconditions.checkNotNull(key, "Key cannot be null");

        final Map<UK, UV> map = value();
        return map != null ? map.get(key) : null;
    }

    /**
     * Associates the given value with the given key.
     *
     * <p>If the value is null, the key is removed from the map.
     *
     * @param key The key to associate the value with
     * @param value The value to associate with the key, or null to remove the key
     * @throws NullPointerException if the key is null
     */
    @Override
    public void put(final UK key, final UV value) throws IOException {
        Preconditions.checkNotNull(key, "Key cannot be null");

        Map<UK, UV> map = value();
        if (map == null) {
            map = new HashMap<>();
        }

        if (value == null) {
            map.remove(key);
        } else {
            map.put(key, value);
        }

        update(map);
    }

    /**
     * Adds all key-value pairs from the given map to this map state.
     *
     * @param map The map of key-value pairs to add
     * @throws NullPointerException if the map is null
     */
    @Override
    public void putAll(final Map<UK, UV> map) throws IOException {
        Preconditions.checkNotNull(map, "Map cannot be null");

        if (map.isEmpty()) {
            return;
        }

        Map<UK, UV> currentMap = value();
        if (currentMap == null) {
            currentMap = new HashMap<>();
        }

        currentMap.putAll(map);
        update(currentMap);
    }

    /**
     * Removes the key-value pair with the given key from this map state.
     *
     * @param key The key to remove
     * @throws NullPointerException if the key is null
     */
    @Override
    public void remove(final UK key) throws IOException {
        Preconditions.checkNotNull(key, "Key cannot be null");

        final Map<UK, UV> map = value();
        if (map != null && map.containsKey(key)) {
            map.remove(key);
            update(map);
        }
    }

    /**
     * Returns whether this map state contains the given key.
     *
     * @param key The key to check
     * @return True if the map contains the key, false otherwise
     * @throws NullPointerException if the key is null
     */
    @Override
    public boolean contains(final UK key) throws IOException {
        Preconditions.checkNotNull(key, "Key cannot be null");

        final Map<UK, UV> map = value();
        return map != null && map.containsKey(key);
    }

    /**
     * Returns an iterable over all key-value pairs in this map state.
     *
     * @return An iterable over all key-value pairs
     */
    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws IOException {
        final Map<UK, UV> map = value();
        return map != null ? map.entrySet() : Collections.emptySet();
    }

    /**
     * Returns an iterable over all keys in this map state.
     *
     * @return An iterable over all keys
     */
    @Override
    public Iterable<UK> keys() throws IOException {
        final Map<UK, UV> map = value();
        return map != null ? map.keySet() : Collections.emptySet();
    }

    /**
     * Returns an iterable over all values in this map state.
     *
     * @return An iterable over all values
     */
    @Override
    public Iterable<UV> values() throws IOException {
        final Map<UK, UV> map = value();
        return map != null ? map.values() : Collections.emptyList();
    }

    /**
     * Returns an iterator over all key-value pairs in this map state.
     *
     * @return An iterator over all key-value pairs
     */
    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws IOException {
        final Map<UK, UV> map = value();
        return map != null ? map.entrySet().iterator() : Collections.emptyIterator();
    }

    /**
     * Returns whether this map state is empty.
     *
     * @return True if the map is empty, false otherwise
     */
    @Override
    public boolean isEmpty() throws IOException {
        final Map<UK, UV> map = value();
        return map == null || map.isEmpty();
    }
}
