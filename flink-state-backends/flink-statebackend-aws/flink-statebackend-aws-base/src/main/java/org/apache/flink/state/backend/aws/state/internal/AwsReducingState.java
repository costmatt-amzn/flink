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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * AWS implementation of {@link ReducingState}.
 *
 * <p>This class provides an AWS-backed implementation of Flink's {@link ReducingState} interface,
 * which stores a single value that is the result of an incremental aggregation using a {@link ReduceFunction}.
* The value is stored in an AWS service (like DynamoDB or S3) using the composite key structure
 * defined in the AWS state backend.
 *
 * <p>The state is accessed and modified using the standard {@link ReducingState} methods:
 * <ul>
 *   <li>{@link #get()} - Retrieves the current aggregated value from the AWS service</li>
 *   <li>{@link #add(Object)} - Adds a new value to the aggregation</li>
 *   <li>{@link #clear()} - Removes the aggregated value from the AWS service</li>
 * </ul>
 *
 * <p>This implementation extends {@link AwsStateBase} to leverage common functionality
 * for AWS state operations, including serialization, deserialization, and key building.
 * The actual storage operations are delegated to the specific AWS service implementation
 * in the backend.
 *
 * <p>Performance considerations:
 * <ul>
 *   <li>Each {@link #add(Object)} operation requires a read followed by a write to the AWS service</li>
 *   <li>Consider batching updates when possible to reduce network calls</li>
 *   <li>The reduce function is executed locally after retrieving the current value</li>
 * </ul>
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value being aggregated.
 *
 * @see ReducingState
 * @see AwsStateBase
 */
public final class AwsReducingState<K, N, V> extends AwsStateBase<K, N, V> implements ReducingState<V> {

    /** The reduce function used for aggregation. */
    private final ReduceFunction<V> reduceFunction;

    /**
     * Creates a new AWS-backed ReducingState.
     *
     * <p>This constructor initializes a ReducingState implementation that stores its data in an AWS service.
     * It sets up the necessary serializers and key builders for state operations.
     *
     * @param backend The AWS keyed state backend that manages this state
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param sharedKeyBuilder The shared key builder for constructing composite keys
     * @param stateDescriptor The state descriptor containing configuration for this state
     * @param reduceFunction The reduce function used for aggregation
     */
    public AwsReducingState(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, V> stateDescriptor,
            final ReduceFunction<V> reduceFunction) {
        super(
                backend,
                keySerializer,
                namespaceSerializer,
                sharedKeyBuilder,
                stateDescriptor
        );
        this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
    }

    /**
     * Returns the current aggregated value.
     *
     * @return The current aggregated value, or null if no value has been added yet
     */
    @Override
    public V get() throws IOException {
        return value();
    }

    /**
     * Adds a new value to the aggregation.
     *
     * <p>If this is the first value added, it becomes the aggregated value.
     * Otherwise, the new value is combined with the current aggregated value
     * using the reduce function.
     *
     * @param value The value to add
     * @throws NullPointerException if the value is null
     * @throws RuntimeException if an error occurs during the reduce operation
     */
    @Override
    public void add(final V value) throws IOException {
        Preconditions.checkNotNull(value, "Value cannot be null");

        final V currentValue = value();
        try {
            if (currentValue == null) {
                update(value);
            } else {
                update(reduceFunction.reduce(currentValue, value));
            }
        } catch (final Exception e) {
            throw new RuntimeException("Error adding to reducing state", e);
        }
    }
}
