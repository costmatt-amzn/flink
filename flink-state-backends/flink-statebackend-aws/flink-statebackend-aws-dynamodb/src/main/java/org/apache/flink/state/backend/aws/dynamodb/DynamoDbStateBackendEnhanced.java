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

package org.apache.flink.state.backend.aws.dynamodb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.backend.aws.AbstractAwsKeyedStateBackendBuilder;
import org.apache.flink.state.backend.aws.AbstractAwsStateBackend;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;
import org.apache.flink.state.backend.aws.dynamodb.util.DynamoDbTableUtils;
import org.apache.flink.state.backend.aws.util.AWSClientUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.Properties;

/**
 * State backend implementation that stores state in Amazon DynamoDB using the Enhanced Client.
 *
 * <p>This state backend uses the DynamoDB Enhanced Client to provide a more object-oriented
 * approach to working with DynamoDB, with automatic mapping between Java objects and DynamoDB items.
 */
public class DynamoDbStateBackendEnhanced extends DynamoDbStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStateBackendEnhanced.class);

    /**
     * Creates a new DynamoDB state backend with the given configuration.
     *
     * @param configuration The configuration for the DynamoDB state backend
     */
    public DynamoDbStateBackendEnhanced(final Configuration configuration) {
        super(configuration);
    }

    /**
     * Creates a new DynamoDB state backend with the given client and configuration.
     *
     * @param client The DynamoDB client
     * @param configuration The configuration
     */
    public DynamoDbStateBackendEnhanced(final DynamoDbClient client, final Configuration configuration) {
        super(client, configuration);
    }

    /**
     * Creates a new DynamoDB state backend with the given client and table.
     *
     * @param client The DynamoDB client
     * @param table The DynamoDB table description
     */
    public DynamoDbStateBackendEnhanced(final DynamoDbClient client, final TableDescription table) {
        super(client, table);
    }

    @Override
    public <K> AbstractAwsKeyedStateBackendBuilder<K, DynamoDbClient, TableDescription, ?> createKeyedStateBackendBuilder(
            final DynamoDbClient safeAwsClient,
            final TableDescription safeAwsModel,
            final KeyedStateBackendParameters<K> parameters) {
        LOG.info(
                "Creating DynamoDB Enhanced keyed state backend for operator {} in job {}",
                parameters.getOperatorIdentifier(),
                parameters.getJobID());

        return new DynamoDbKeyedStateBackendEnhancedBuilder<>(
                safeAwsClient,
                safeAwsModel,
                parameters.getEnv().getJobInfo(),
                parameters.getEnv().getTaskInfo(),
                parameters.getOperatorIdentifier(),
                configuration,
                parameters.getEnv().getTaskStateManager().createLocalRecoveryConfig(),
                parameters.getKvStateRegistry(),
                parameters.getKeySerializer(),
                parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                parameters.getNumberOfKeyGroups(),
                parameters.getKeyGroupRange(),
                parameters.getEnv().getExecutionConfig(),
                parameters.getTtlTimeProvider(),
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build(),
                getCompressionDecorator(parameters.getEnv().getExecutionConfig()),
                parameters.getStateHandles(),
                parameters.getCancelStreamRegistry(),
                parameters.getMetricGroup());
    }

    /**
     * Creates a new state backend with the given configuration.
     *
     * @param config The configuration to merge with the current configuration
     * @param classLoader The class loader to use for loading classes
     * @return A new DynamoDB state backend with the merged configuration
     */
    @Override
    public DynamoDbStateBackendEnhanced configure(
            final ReadableConfig config,
            final ClassLoader classLoader) {
        final Configuration mergedConfig = new Configuration();
        mergedConfig.addAll(this.configuration);
        mergedConfig.addAll(Configuration.fromMap(config.toMap()));

        return new DynamoDbStateBackendEnhanced(mergedConfig);
    }
}
