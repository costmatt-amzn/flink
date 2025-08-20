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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;

/**
 * AWS implementation of {@link AggregatingState}.
 *
 * <p>This class provides an AWS-backed implementation of Flink's {@link AggregatingState} interface,
 * which stores a single value that is the result of an incremental aggregation using an {@link AggregateFunction}.
* The accumulator is stored in an AWS service (like DynamoDB or S3) using the composite key structure
 * defined in the AWS state backend.
 *
 * <p>The state is accessed and modified using the standard {@link AggregatingState} methods:
 * <ul>
 *   <li>{@link #get()} - Retrieves the current aggregated result from the AWS service</li>
 *   <li>{@link #add(Object)} - Adds a new value to the aggregation</li>
 *   <li>{@link #clear()} - Removes the accumulator from the AWS service</li>
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
 *   <li>The aggregate function is executed locally after retrieving the current accumulator</li>
 * </ul>
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the input values.
 * @param <ACC> The type of the accumulator.
 * @param <OUT> The type of the output result.
 *
 * @see AggregatingState
 * @see AwsStateBase
 */
public final class AwsAggregatingState<K, N, IN, ACC, OUT>
        extends AwsStateBase<K, N, ACC>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    /** The aggregate function used for aggregation. */
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    /**
     * Creates a new AWS-backed AggregatingState.
     *
     * <p>This constructor initializes an AggregatingState implementation that stores its data in an AWS service.
     * It sets up the necessary serializers and key builders for state operations.
     *
     * @param backend The AWS keyed state backend that manages this state
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param sharedKeyBuilder The shared key builder for constructing composite keys
     * @param stateDescriptor The state descriptor containing configuration for this state
     * @param aggregateFunction The aggregate function used for aggregation
     */
    public AwsAggregatingState(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, ACC> stateDescriptor,
            final AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        super(
                backend,
                keySerializer,
                namespaceSerializer,
                sharedKeyBuilder,
                stateDescriptor
        );
        this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
    }

    /**
     * Returns the current aggregated result.
     *
     * <p>This method retrieves the current accumulator from the AWS service and applies
     * the aggregate function's getResult method to produce the output value.
     *
     * @return The current aggregated result, or null if no value has been added yet
     * @throws IOException If an error occurs while retrieving the accumulator
     */
    @Override
    public OUT get() throws IOException {
        final ACC accumulator = value();
        if (accumulator == null) {
            return null;
        }
        return aggregateFunction.getResult(accumulator);
    }

    /**
     * Adds a new value to the aggregation.
     *
     * <p>This method retrieves the current accumulator from the AWS service, creates a new one
     * if none exists, adds the new value to the accumulator using the aggregate function,
     * and stores the updated accumulator back in the AWS service.
     *
     * @param value The value to add
     * @throws NullPointerException if the value is null
     * @throws IOException If an error occurs while accessing or updating the accumulator
     */
    @Override
    public void add(final IN value) throws IOException {
        Preconditions.checkNotNull(value, "Value cannot be null");

        ACC accumulator = value();
        if (accumulator == null) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(value, accumulator);
        update(accumulator);
    }

    /**
     * Merges multiple namespaces into a target namespace.
     *
     * <p>This method retrieves accumulators from multiple source namespaces,
     * merges them using the aggregate function's merge method, and stores
     * the result in the target namespace.
     *
     * @param target The target namespace
     * @param sources The source namespaces to merge
     * @throws Exception If an error occurs during the merge operation
     */
    @Override
    public void mergeNamespaces(final N target, final Collection<N> sources) throws Exception {
        ACC merged = getInternal();
        for (final N source : sources) {
            setCurrentNamespace(source);
            final ACC acc = getInternal();
            if (acc != null) {
                if (merged == null) {
                    merged = acc;
                } else {
                    merged = aggregateFunction.merge(merged, acc);
                }
            }
        }

        update(merged);
    }

    @Override
    public ACC getInternal() throws Exception {
        return value();
    }

    @Override
    public void updateInternal(final ACC valueToStore) throws Exception {
        update(valueToStore);
    }
}
