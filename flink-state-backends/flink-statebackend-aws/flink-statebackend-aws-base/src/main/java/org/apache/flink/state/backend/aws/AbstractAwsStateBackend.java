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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPojo;

import java.io.IOException;

/**
 * Abstract base class for AWS-based state backends.
 *
 * <p>This class provides common functionality for state backends that use AWS services
 * for state storage. It handles configuration management and implements common methods
 * from the {@link AbstractStateBackend} and {@link ConfigurableStateBackend} interfaces.
 *
 * <p>Concrete implementations must provide service-specific functionality for creating
 * keyed state backends and handling AWS client creation and management.
 *
 * <p>The AWS state backends enable Flink applications to leverage AWS managed services for state storage, offering:
 * <ul>
 *   <li>Scalable and durable state storage using AWS services</li>
 *   <li>Configurable AWS credentials and region settings</li>
 *   <li>Support for all standard Flink state types</li>
 *   <li>Optimized performance with caching and batching</li>
 *   <li>Testing support with local service emulation</li>
 * </ul>
 *
 * @param <C> The type of the AWS client.
 * @param <M> The type of the AWS model object that contains service-specific configuration.
 */
public abstract class AbstractAwsStateBackend<C extends SdkClient, M extends SdkPojo>
        extends AbstractStateBackend
        implements ConfigurableStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractAwsStateBackend.class);

    /** The configuration for the state backend. */
    protected final Configuration configuration;

    /**
     * Creates a new AWS state backend with the given configuration.
     *
     * <p>This constructor creates a new AWS client using the provided configuration.
     *
     * @param configuration The configuration for the state backend, including AWS credentials,
     *         region, and service-specific settings.
     */
    public AbstractAwsStateBackend(final Configuration configuration) {
        this.configuration = Preconditions.checkNotNull(configuration);
        LOG.info("Initializing AWS state backend with configuration");
    }

    /**
     * Creates a new AWS state backend with the given client and configuration.
     *
     * <p>This constructor allows providing a custom AWS client, which can be useful
     * for testing or when special handling of AWS operations is required.
     *
     * @param awsClient The AWS client for interacting with AWS services.
     * @param configuration The configuration for the state backend.
     */
    public AbstractAwsStateBackend(
            final C awsClient,
            final M awsModel,
            final Configuration configuration) {
        this.configuration = Preconditions.checkNotNull(configuration);
        LOG.info("Initializing AWS state backend with provided client and model");
    }

    /**
     * Creates an AWS client for the specific service.
     *
     * <p>This method must be implemented by concrete subclasses to create the appropriate
     * AWS client for the specific service (S3, DynamoDB, etc.).
     *
     * @param configuration The configuration for creating the AWS client.
     *
     * @return A new AWS client.
     */
    protected abstract C createAwsClient(Configuration configuration);

    /**
     * Creates an AWS model object for the specific service.
     *
     * <p>This method must be implemented by concrete subclasses to create the appropriate
     * AWS model object for the specific service (e.g., TableDescription for DynamoDB,
     * BucketConfiguration for S3).
     *
     * <p>The model object typically contains service-specific configuration and metadata
     * that is needed for state operations.
     *
     * @param safeAwsClient The AWS client for interacting with AWS services.
     * @param configuration The configuration for creating the AWS model.
     *
     * @return A new AWS model object.
     */
    protected abstract M createAwsModel(C safeAwsClient, Configuration configuration);

    /**
     * Gets the configuration for the state backend.
     *
     * <p>This configuration contains all settings for the state backend, including
     * AWS credentials, region, and service-specific settings.
     *
     * @return The configuration object containing all settings for this state backend.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Creates an operator state backend.
     *
     * <p>This implementation delegates to the {@link DefaultOperatorStateBackendBuilder} to create
     * a default operator state backend. AWS state backends currently do not provide
     * specialized implementations for operator state.
     *
     * @param parameters Parameters for creating the operator state backend.
     *
     * @return A new operator state backend.
     *
     * @throws Exception If an error occurs while creating the state backend.
     */
    @Override
    public OperatorStateBackend createOperatorStateBackend(
            final OperatorStateBackendParameters parameters) throws Exception {
        LOG.debug("Creating operator state backend");
        final boolean asyncSnapshots = true;
        final OperatorStateBackend backend = new DefaultOperatorStateBackendBuilder(
                parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                parameters.getEnv().getExecutionConfig(),
                asyncSnapshots,
                parameters.getStateHandles(),
                parameters.getCancelStreamRegistry())
                .build();
        LOG.debug("Operator state backend created successfully");
        return backend;
    }

    /**
     * Creates a keyed state backend that is backed by AWS services.
     *
     * <p>This method creates a new {@link AbstractKeyedStateBackend} for the given operator.
     *
     * @param parameters Parameters for creating the keyed state backend, including operator
     *         identifier, key serializer, and execution environment.
     * @param <K> The type of the keys in the state backend.
     *
     * @return A new keyed state backend.
     *
     * @throws IOException If an error occurs while creating the state backend.
     */
    @Override
    public final <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            final StateBackend.KeyedStateBackendParameters<K> parameters) throws IOException {
        LOG.info("Creating keyed state backend for operator: {}", parameters.getOperatorIdentifier());
        try {
            final C awsClient = createAwsClient(configuration);
            LOG.debug("AWS client created successfully");

            final M awsModel = createAwsModel(awsClient, configuration);
            LOG.debug("AWS model created successfully");

            AbstractKeyedStateBackend<K> backend = createKeyedStateBackendBuilder(awsClient, awsModel, parameters).build();
            LOG.info("Keyed state backend created successfully for operator: {}", parameters.getOperatorIdentifier());
            return backend;
        } catch (Exception e) {
            LOG.error("Failed to create keyed state backend for operator: {}", parameters.getOperatorIdentifier(), e);
            throw e;
        }
    }

    public abstract <K>
    AbstractAwsKeyedStateBackendBuilder<K, C, M, ? extends AwsKeyedStateBackendBase<K, C, M>> createKeyedStateBackendBuilder(
            C safeAwsClient,
            M safeAwsModel,
            StateBackend.KeyedStateBackendParameters<K> parameters);

    /**
     * Creates a new state backend with the given configuration.
     *
     * <p>This method creates a new state backend by merging the current configuration
     * with the provided configuration. This allows for overriding specific configuration options
     * while keeping the rest of the configuration intact.
     *
     * <p>This method must be implemented by concrete subclasses to create a new instance
     * of the specific state backend type.
     *
     * @param config The configuration to merge with the current configuration.
     * @param classLoader The class loader to use for loading classes.
     *
     * @return A new state backend with the merged configuration.
     */
    @Override
    public abstract AbstractAwsStateBackend<C, M> configure(
            ReadableConfig config,
            ClassLoader classLoader);
}
