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

package org.apache.flink.state.backend.aws.dynamodb;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.AbstractAwsKeyedStateBackendBuilder;
import org.apache.flink.state.backend.aws.dynamodb.snapshot.DynamoDbSnapshotStrategy;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * Builder class for {@link DynamoDbKeyedStateBackend}.
 *
 * <p>This builder creates and configures a DynamoDB-backed keyed state backend.
 * It handles the initialization of all required components and sets up the state
 * backend with the appropriate configuration.
 *
 * <p>The builder follows the standard Flink state backend builder pattern and
 * integrates with the Flink state management infrastructure.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class DynamoDbKeyedStateBackendBuilder<K> extends AbstractAwsKeyedStateBackendBuilder<K, DynamoDbClient, TableDescription, DynamoDbKeyedStateBackend<K>> {

    /**
     * Creates a new {@link DynamoDbKeyedStateBackendBuilder}.
     *
     * <p>This constructor initializes the builder with all required components
     * for creating a DynamoDB-backed keyed state backend.
     *
     * @param dynamoDbClient The DynamoDB client for state operations.
     * @param table The DynamoDB table description for the table storing state.
     * @param jobInfo The information about the current job, including job ID and other metadata.
     * @param operatorIdentifier The identifier for the operator that will use this state backend.
     * @param configuration The configuration containing DynamoDB-specific settings and credentials.
     * @param localRecoveryConfig The configuration for local state recovery.
     * @param kvStateRegistry The registry for key-value state used for queryable state.
     * @param keySerializer The serializer for the state keys.
     * @param userCodeClassLoader The class loader for user code.
     * @param numberOfKeyGroups The total number of key groups.
     * @param keyGroupRange The key group range assigned to this backend.
     * @param executionConfig The execution configuration.
     * @param ttlTimeProvider The TTL time provider for state expiration.
     * @param latencyTrackingStateConfig The configuration for latency tracking.
     * @param restoreStateHandles The state handles to restore from.
     * @param cancelStreamRegistry The registry for cancellable streams.
     * @param metricGroup The metric group for metrics reporting.
     */
    public DynamoDbKeyedStateBackendBuilder(
            final DynamoDbClient dynamoDbClient,
            final TableDescription table,
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final String operatorIdentifier,
            final Configuration configuration,
            final LocalRecoveryConfig localRecoveryConfig,
            final TaskKvStateRegistry kvStateRegistry,
            final TypeSerializer<K> keySerializer,
            final ClassLoader userCodeClassLoader,
            final int numberOfKeyGroups,
            final KeyGroupRange keyGroupRange,
            final ExecutionConfig executionConfig,
            final TtlTimeProvider ttlTimeProvider,
            final LatencyTrackingStateConfig latencyTrackingStateConfig,
            final StreamCompressionDecorator streamCompressionDecorator,
            final Collection<KeyedStateHandle> restoreStateHandles,
            final CloseableRegistry cancelStreamRegistry,
            final MetricGroup metricGroup) {
        super(
                dynamoDbClient,
                table,
                jobInfo,
                taskInfo,
                operatorIdentifier,
                configuration,
                localRecoveryConfig,
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                streamCompressionDecorator,
                restoreStateHandles,
                cancelStreamRegistry,
                metricGroup);
    }

    @Override
    protected AwsSnapshotStrategy<K, DynamoDbClient, TableDescription> createSnapshotStrategy() {
        return new DynamoDbSnapshotStrategy<>(
                awsClient,
                awsModel,
                backendUID,
                keyGroupRange,
                localRecoveryConfig,
                keySerializerProvider.currentSchemaSerializer(),
                kvStateInformation,
                metricGroup);
    }

    /**
     * Builds and returns a new {@link DynamoDbKeyedStateBackend} instance.
     *
     * <p>This method creates a new DynamoDB-backed keyed state backend with the
     * configuration provided to this builder. It initializes all required components,
     * including the key context, serialized composite key builder, and snapshot strategy.
     *
     * <p>The method generates a unique backend ID for the state backend instance
     * and sets up the necessary data structures for state management.
     *
     * @return A new {@link DynamoDbKeyedStateBackend} instance.
     * @throws BackendBuildingException If the backend cannot be built.
     */
    @Nonnull
    @Override
    public DynamoDbKeyedStateBackend<K> build() throws BackendBuildingException {
        // Create the DynamoDB keyed state backend
        return new DynamoDbKeyedStateBackend<>(
                awsClient,
                awsModel,
                jobInfo,
                taskInfo,
                operatorIdentifier,
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                sharedKeyBuilder,
                keyGroupCompressionDecorator,
                priorityQueueSetFactory,
                keyContext,
                registeredPQStates,
                createSnapshotStrategy(),
                kvStateInformation,
                keyGroupPrefix,
                executionConfig.isUseSnapshotCompression(),
                metricGroup);
    }
}