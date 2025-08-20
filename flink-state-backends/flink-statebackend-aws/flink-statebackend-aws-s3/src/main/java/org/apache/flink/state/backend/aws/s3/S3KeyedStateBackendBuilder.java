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

package org.apache.flink.state.backend.aws.s3;

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
import org.apache.flink.state.backend.aws.s3.snapshot.S3SnapshotStrategy;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.util.IOUtils;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * Builder class for {@link S3KeyedStateBackend}.
 *
 * @param <K> The data type of the key.
 */
public class S3KeyedStateBackendBuilder<K> extends AbstractAwsKeyedStateBackendBuilder<K, S3Client, Bucket, S3KeyedStateBackend<K>> {

    /** The configuration for the S3 state backend. */
    private final Configuration configuration;

    /**
     * Creates a new S3KeyedStateBackendBuilder.
     *
     * @param s3Client The S3 client.
     * @param operatorIdentifier
     * @param configuration The configuration for the S3 state backend.
     * @param localRecoveryConfig The local recovery config.
     * @param kvStateRegistry The KV state registry.
     * @param keySerializer The key serializer.
     * @param userCodeClassLoader The user code class loader.
     * @param numberOfKeyGroups The number of key groups.
     * @param keyGroupRange The key group range.
     * @param executionConfig The execution config.
     * @param ttlTimeProvider The TTL time provider.
     * @param latencyTrackingStateConfig The latency tracking state config.
     * @param streamCompressionDecorator The stream compression decorator.
     * @param stateHandles The state handles.
     * @param cancelStreamRegistry The cancel stream registry.
     */
    public S3KeyedStateBackendBuilder(
            final S3Client s3Client,
            final Bucket bucket,
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
            final Collection<KeyedStateHandle> stateHandles,
            final CloseableRegistry cancelStreamRegistry,
            final MetricGroup metricGroup) {
        super(
                s3Client,
                bucket,
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
                stateHandles,
                cancelStreamRegistry,
                metricGroup);
        this.configuration = configuration;
    }

    @Override
    protected AwsSnapshotStrategy<K, S3Client, Bucket> createSnapshotStrategy() {
        return new S3SnapshotStrategy<>(
                awsClient,
                awsModel,
                backendUID,
                keyGroupRange,
                localRecoveryConfig,
                keySerializerProvider.currentSchemaSerializer(),
                kvStateInformation,
                metricGroup);
    }

    @Nonnull
    @Override
    public S3KeyedStateBackend<K> build() throws BackendBuildingException {
        try {
            return new S3KeyedStateBackend<>(
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
                    configuration,
                    sharedKeyBuilder,
                    keyGroupCompressionDecorator,
                    priorityQueueSetFactory,
                    keyContext,
                    createSnapshotStrategy(),
                    registeredPQStates,
                    kvStateInformation,
                    keyGroupPrefix,
                    executionConfig.isUseSnapshotCompression(),
                    metricGroup);
        } catch (final Throwable t) {
            // Clean up
            IOUtils.closeQuietly(awsClient);
            throw new BackendBuildingException("Failed to build S3 keyed state backend", t);
        }
    }
}