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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.state.backend.aws.AwsKeyedStateBackendBase;

/**
 * AWS implementation of {@link ValueState}.
 *
 * <p>This class provides an AWS-backed implementation of Flink's {@link ValueState} interface,
 * which stores a single value associated with a key and namespace. The value is stored in an AWS
 * service (like DynamoDB or S3) using the composite key structure defined in the AWS state backend.
 *
 * <p>The state is accessed and modified using the standard {@link ValueState} methods:
 * <ul>
 *   <li>{@link #value()} - Retrieves the current value from the AWS service</li>
 *   <li>{@link #update(Object)} - Updates the value in the AWS service</li>
 *   <li>{@link #clear()} - Removes the value from the AWS service</li>
 * </ul>
 *
 * <p>This implementation extends {@link AwsStateBase} to leverage common functionality
 * for AWS state operations, including serialization, deserialization, and key building.
 * The actual storage operations are delegated to the specific AWS service implementation
 * in the backend.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 *
 * @see ValueState
 * @see AwsStateBase
 */
public final class AwsValueState<K, N, V> extends AwsStateBase<K, N, V> implements ValueState<V> {

    /**
     * Creates a new AWS-backed ValueState.
     *
     * <p>This constructor initializes a ValueState implementation that stores its data in an AWS service.
     * It sets up the necessary serializers and key builders for state operations.
     *
     * @param backend The AWS keyed state backend that manages this state
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param sharedKeyBuilder The shared key builder for constructing composite keys
     * @param stateDescriptor The state descriptor containing configuration for this state
     */
    public AwsValueState(
            final AwsKeyedStateBackendBase<K, ?, ?> backend,
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final SerializedCompositeKeyBuilder<K> sharedKeyBuilder,
            final StateDescriptor<?, V> stateDescriptor) {
        super(
                backend,
                keySerializer,
                namespaceSerializer,
                sharedKeyBuilder,
                stateDescriptor
        );
    }
}
