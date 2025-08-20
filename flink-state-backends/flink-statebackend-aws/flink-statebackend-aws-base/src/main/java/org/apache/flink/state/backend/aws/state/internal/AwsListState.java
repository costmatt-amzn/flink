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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * AWS implementation of {@link ListState}.
 *
 * <p>This class provides an AWS-backed implementation of Flink's {@link ListState} interface,
 * which stores a list of elements associated with a key and namespace. The list is stored as a
* serialized byte array in an AWS service (like DynamoDB or S3) using the composite key structure
 * defined in the AWS state backend.
 *
 * <p>The state is accessed and modified using the standard {@link ListState} methods:
 * <ul>
 *   <li>{@link #get()} - Retrieves the current list from the AWS service</li>
 *   <li>{@link #add(Object)} - Adds a single element to the list</li>
 *   <li>{@link #addAll(List)} - Adds multiple elements to the list</li>
 *   <li>{@link #update(List)} - Replaces the entire list</li>
 *   <li>{@link #clear()} - Removes the list from the AWS service</li>
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
 *   <li>For large lists, consider using batching when possible</li>
 *   <li>The entire list is serialized and stored as a single value</li>
 * </ul>
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the elements in the list.
 *
 * @see ListState
 * @see AwsStateBase
 */
public final class AwsListState<K, N, V>
        extends AwsStateBase<K, N, List<V>>
        implements InternalListState<K, N, V> {

    /**
     * Creates a new AWS-backed ListState.
     *
     * <p>This constructor initializes a ListState implementation that stores its data in an AWS service.
     * It sets up the necessary serializers and key builders for state operations.
     *
     * @param backend The AWS keyed state backend that manages this state
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param sharedKeyBuilder The shared key builder for constructing composite keys
     * @param stateDescriptor The state descriptor containing configuration for this state
     */
    public AwsListState(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, List<V>> stateDescriptor) {
        super(
                backend,
                keySerializer,
                namespaceSerializer,
                sharedKeyBuilder,
                stateDescriptor
        );
    }

    /**
     * Adds all elements from the given list to the state list.
     *
     * <p>If the provided list is null or empty, this operation is a no-op.
     *
     * @param values The list of values to add
     */
    @Override
    public void addAll(final List<V> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }

        List<V> currentValues = getInternal();
        if (currentValues == null) {
            currentValues = new ArrayList<>();
        }
        currentValues.addAll(values);
        update(currentValues);
    }

    /**
     * Adds a single value to the state list.
     *
     * <p>If the provided value is null, this operation is a no-op.
     *
     * @param value The value to add
     */
    @Override
    public void add(final V value) throws Exception {
        if (value == null) {
            return;
        }

        List<V> currentValues = getInternal();
        if (currentValues == null) {
            currentValues = new ArrayList<>();
        }
        currentValues.add(value);
        update(currentValues);
    }

    /**
     * Returns the current list of values in the state.
     *
     * <p>If the state does not exist, an empty list is returned.
     *
     * @return An iterable over the current list of values
     */
    @Override
    public Iterable<V> get() throws Exception {
        final List<V> result = getInternal();
        return result != null ? result : Collections.emptyList();
    }

    @Override
    public void mergeNamespaces(final N target, final Collection<N> sources) throws Exception {
        final List<V> merged = new ArrayList<>();
        for (final N source : sources) {
            setCurrentNamespace(source);
            final List<V> sourceState = getInternal();
            if (sourceState != null) {
                merged.addAll(sourceState);
            }
        }

        update(merged);
    }

    @Override
    public List<V> getInternal() throws Exception {
        return value();
    }

    @Override
    public void updateInternal(final List<V> values) throws Exception {
        update(values);
    }
}
