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

package org.apache.flink.state.backend.aws;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.backend.aws.snapshot.AwsSnapshotStrategy;
import org.apache.flink.metrics.MetricGroup;

import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPojo;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Abstract builder class for AWS-based keyed state backends.
 *
 * <p>This class provides common functionality for building keyed state backends that use AWS services
 * for state storage. It handles the creation of key contexts, serialized composite key builders,
 * and other common components.
 *
 * <p>The builder pattern is used to create configured instances of AWS keyed state backends with
 * all necessary dependencies properly initialized. This includes:
 * <ul>
 *   <li>Key context and serialization setup</li>
 *   <li>AWS client configuration and management</li>
 *   <li>State metadata tracking</li>
 *   <li>Priority queue state handling</li>
 *   <li>Snapshot strategy creation</li>
 * </ul>
 *
 * <p>Concrete implementations must provide service-specific functionality for creating
 * the actual keyed state backend instance and snapshot strategy.
 *
 * @param <K> The type of the keys in the state backend.
 * @param <C> The type of the AWS client (e.g., DynamoDbClient, S3Client).
 * @param <M> The type of the AWS model object (e.g., TableDescription, Bucket).
 * @param <B> The type of the keyed state backend being built.
 */
public abstract class AbstractAwsKeyedStateBackendBuilder<K, C extends SdkClient, M extends SdkPojo, B extends AwsKeyedStateBackendBase<K, C, M>>
        extends AbstractKeyedStateBackendBuilder<K> {

    /**
     * The AWS client for interacting with AWS services.
     */
    protected final C awsClient;

    /**
     * The configuration of local recovery.
     */
    protected final LocalRecoveryConfig localRecoveryConfig;

    protected final M awsModel;

    protected final UUID backendUID;

    protected final int keyGroupPrefix;

    protected final InternalKeyContext<K> keyContext;

    protected final SerializedCompositeKeyBuilder<K> sharedKeyBuilder;

    protected final LinkedHashMap<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> kvStateInformation;

    protected final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;

    protected final PriorityQueueSetFactory priorityQueueSetFactory;

    protected final JobInfo jobInfo;

    protected final TaskInfo taskInfo;

    protected final String operatorIdentifier;
    protected final Configuration configuration;

    /** The metric group for this state backend. */
    protected final MetricGroup metricGroup;

    /**
     * Creates a new AWS keyed state backend builder.
     *
     * <p>This constructor initializes the builder with all required components for creating
     * an AWS-backed keyed state backend. It sets up the key context, serialized composite key builder,
     * and other common components needed by all AWS state backend implementations.
     *
     * @param awsClient The AWS client for state operations (e.g., DynamoDbClient, S3Client).
     * @param awsModel The AWS model object containing service-specific configuration (e.g., TableDescription for DynamoDB).
     * @param jobInfo The information about the current job, including job ID and other metadata.
     * @param operatorIdentifier The identifier for the operator that will use this state backend.
     * @param configuration The configuration containing AWS-specific settings and credentials.
     * @param localRecoveryConfig The configuration for local state recovery.
     * @param kvStateRegistry The registry for key-value state used for queryable state.
     * @param keySerializer The serializer for the state keys.
     * @param userCodeClassLoader The class loader for user code to ensure proper class resolution.
     * @param numberOfKeyGroups The total number of key groups across all parallel instances.
     * @param keyGroupRange The range of key groups assigned to this backend instance.
     * @param executionConfig The execution configuration with serialization settings.
     * @param ttlTimeProvider The provider for time-to-live functionality in state.
     * @param latencyTrackingStateConfig The configuration for tracking state access latency.
     * @param streamCompressionDecorator The decorator for stream compression during state serialization.
     * @param stateHandles The state handles to restore from during recovery.
     * @param cancelStreamRegistry The cancel stream registry.
     */
    public AbstractAwsKeyedStateBackendBuilder(
            final C awsClient,
            final M awsModel,
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
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                streamCompressionDecorator,
                cancelStreamRegistry);
        this.awsClient = awsClient;
        this.awsModel = awsModel;
        this.operatorIdentifier = operatorIdentifier;
        this.configuration = configuration;
        this.jobInfo = jobInfo;
        this.taskInfo = taskInfo;
        this.localRecoveryConfig = localRecoveryConfig;

        // Generate a unique ID for this backend instance
        this.backendUID = UUID.randomUUID();

        // Create key context and key builder
        this.keyContext = new InternalKeyContextImpl<>(
                keyGroupRange,
                numberOfKeyGroups);
        this.keyGroupPrefix = CompositeKeySerializationUtils
                .computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);

        this.sharedKeyBuilder = new SerializedCompositeKeyBuilder<>(
                keySerializerProvider.currentSchemaSerializer(),
                keyGroupPrefix,
                32);
        this.priorityQueueSetFactory = new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
        this.kvStateInformation = new LinkedHashMap<>();
        this.registeredPQStates = new LinkedHashMap<>();
        this.metricGroup = metricGroup;
    }

    protected abstract AwsSnapshotStrategy<K, C, M> createSnapshotStrategy();

    /**
     * Builds and returns a new AWS keyed state backend instance.
     *
     * <p>This method must be implemented by concrete subclasses to create the appropriate
     * keyed state backend instance for the specific AWS service.
     *
     * @return A new AWS keyed state backend instance.
     * @throws BackendBuildingException If the backend cannot be built.
     */
    @Nonnull
    @Override
    public abstract B build() throws BackendBuildingException;
}
