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
import org.apache.flink.runtime.state.StateBackendFactory;

import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPojo;

import java.io.IOException;

/**
 * Abstract factory for creating AWS-based state backends.
 *
 * <p>This factory implements the Flink {@link StateBackendFactory} interface to allow
 * AWS state backends to be instantiated from configuration. It creates a properly configured
 * state backend instance using the provided configuration.
 *
 * <p>Concrete implementations must provide service-specific functionality for creating
 * the actual state backend instance and validating configuration.
 *
 * @param <S> The type of the state backend being created.
 * @param <C> The type of the AWS client.
 */
public abstract class AbstractAwsStateBackendFactory<S extends AbstractAwsStateBackend<C, M>, C extends SdkClient, M extends SdkPojo>
        implements StateBackendFactory<S> {

    /**
     * Creates a state backend from the given configuration.
     *
     * <p>This method implements the {@link StateBackendFactory} interface to create
     * an AWS-based state backend from configuration. It creates a new configuration object
     * with all options from the provided configuration, validates the configuration,
     * and then creates the appropriate state backend instance.
     *
     * <p>The method handles the conversion from Flink's {@link ReadableConfig} interface
     * to a standard {@link Configuration} object that can be used by the AWS state backend.
     *
     * @param config The configuration containing state backend settings.
     * @param classLoader The class loader to use for loading classes.
     * @return A new AWS state backend instance.
     * @throws IOException If an error occurs while creating the state backend.
     */
    @Override
    public S createFromConfig(
            final ReadableConfig config,
            final ClassLoader classLoader) throws IOException {
        // Create a new configuration with all the options from the given configuration
        final Configuration configuration = new Configuration();
        configuration.addAll(Configuration.fromMap(config.toMap()));

        // Validate required configuration
        validateConfiguration(configuration);

        // Create the state backend
        return createStateBackend(configuration);
    }

    /**
     * Validates that the configuration contains all required options.
     *
     * <p>This method must be implemented by concrete subclasses to validate that
     * the configuration contains all required options for the specific AWS service.
     *
     * @param configuration The configuration to validate.
     * @throws IllegalArgumentException If the configuration is invalid.
     */
    protected abstract void validateConfiguration(Configuration configuration) throws IllegalArgumentException;

    /**
     * Creates a new state backend instance with the given configuration.
     *
     * <p>This method must be implemented by concrete subclasses to create the appropriate
     * state backend instance for the specific AWS service.
     *
     * @param configuration The configuration for the state backend.
     * @return A new state backend instance.
     */
    protected abstract S createStateBackend(Configuration configuration);
}
