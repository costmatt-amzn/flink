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

package org.apache.flink.state.backend.aws.dynamodb;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.AbstractAwsKeyedStateBackendBuilder;
import org.apache.flink.state.backend.aws.dynamodb.snapshot.DynamoDbSnapshotStrategy;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.Collection;

/**
 * Builder for the {@link DynamoDbKeyedStateBackendEnhanced}.
 *
 * @param <K> The type of the keys in the state backend.
 */
public class DynamoDbKeyedStateBackendEnhancedBuilder<K>
        extends AbstractAwsKeyedStateBackendBuilder<K, DynamoDbClient, TableDescription, DynamoDbKeyedStateBackendEnhanced<K>> {

    /**
     * Creates a new DynamoDB keyed state backend builder.
     *
     * @param dynamoDbClient The DynamoDB client
     * @param table The DynamoDB table description
     * @param jobInfo The job information
     * @param operatorIdentifier The operator identifier
     * @param configuration The configuration
     * @param localRecoveryConfig The local recovery configuration
     * @param kvStateRegistry The KV state registry
     * @param keySerializer The key serializer
     * @param userCodeClassLoader The user code class loader
     * @param numberOfKeyGroups The number of key groups
     * @param keyGroupRange The key group range
     * @param executionConfig The execution configuration
     * @param ttlTimeProvider The TTL time provider
     * @param latencyTrackingStateConfig The latency tracking state configuration
     * @param compressionDecorator The compression decorator
     * @param stateHandles The state handles
     * @param cancelStreamRegistry The cancel stream registry
     * @param metricGroup The metric group for metrics reporting.
     */
    public DynamoDbKeyedStateBackendEnhancedBuilder(
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
            final org.apache.flink.api.common.ExecutionConfig executionConfig,
            final TtlTimeProvider ttlTimeProvider,
            final LatencyTrackingStateConfig latencyTrackingStateConfig,
            final StreamCompressionDecorator compressionDecorator,
            final Collection<org.apache.flink.runtime.state.KeyedStateHandle> stateHandles,
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
                compressionDecorator,
                stateHandles,
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

    @Override
    public DynamoDbKeyedStateBackendEnhanced<K> build() throws IllegalConfigurationException {

        // Create the keyed state backend
        return new DynamoDbKeyedStateBackendEnhanced<>(
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
