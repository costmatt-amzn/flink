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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.backend.aws.AbstractAwsKeyedStateBackendBuilder;
import org.apache.flink.state.backend.aws.AbstractAwsStateBackend;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendOptions;
import org.apache.flink.state.backend.aws.dynamodb.util.DynamoDbTableUtils;
import org.apache.flink.state.backend.aws.util.AWSClientUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.Properties;

/**
 * State backend implementation that stores state in Amazon DynamoDB.
 *
 * <p>This state backend stores keyed state in DynamoDB tables, automatically creating
 * the required table structure if it doesn't exist. It supports both provisioned
 * throughput and on-demand capacity modes for DynamoDB tables.
 *
 * <p>Key features:
 * <ul>
 *   <li>Durable state storage in DynamoDB with automatic table management</li>
 *   <li>Support for all standard Flink state types (ValueState, ListState, MapState, etc.)</li>
 *   <li>Automatic compression of state data using Snappy</li>
 *   <li>TTL (Time-To-Live) support with configurable expiration policies</li>
 *   <li>Configurable throughput (provisioned or on-demand capacity)</li>
 *   <li>Local DynamoDB support for testing and development</li>
 *   <li>AWS SDK v2.x with async operations for improved performance</li>
 * </ul>
 *
 * <p>The state backend requires configuration for:
 * <ul>
 *   <li>AWS credentials and region</li>
 *   <li>DynamoDB table name</li>
 *   <li>Capacity settings (provisioned or on-demand)</li>
 * </ul>
 *
 * <p>Current limitations:
 * <ul>
 *   <li>Currently does not support operator state</li>
 *   <li>Not recommended for production use yet</li>
 *   <li>Performance characteristics may differ from other state backends</li>
 *   <li>Each state operation requires a network call to DynamoDB</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Configuration config = new Configuration();
 * config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
 * config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
 * config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
 * config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");
 * config.set(DynamoDbStateBackendOptions.TABLE_NAME, "flink-state");
 * config.set(DynamoDbStateBackendOptions.USE_ON_DEMAND, true);
 *
 * DynamoDbStateBackend stateBackend = new DynamoDbStateBackend(config);
 * env.setStateBackend(stateBackend);
 * }</pre>
 *
 * <p>Alternatively, you can configure the state backend in your {@code flink-conf.yaml}:
 * <pre>{@code
 * state.backend: dynamodb
 * state.backend.dynamodb.table.name: flink-state
 * state.backend.dynamodb.table.on-demand: true
 * aws.region: us-west-2
 * aws.credentials.provider: BASIC
 * aws.credentials.provider.basic.accesskeyid: your-access-key
 * aws.credentials.provider.basic.secretkey: your-secret-key
 * }</pre>
 *
 * @see org.apache.flink.state.backend.aws.config.AWSConfigOptions
 * @see org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendOptions
 */
public class DynamoDbStateBackend extends AbstractAwsStateBackend<DynamoDbClient, TableDescription> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStateBackend.class);

    /**
     * Creates a new DynamoDB state backend with the given configuration.
     *
     * <p>This constructor creates a new DynamoDB client using the provided configuration
     * and automatically creates or validates the DynamoDB table for state storage.
     * The configuration must include AWS credentials, region, and DynamoDB table name.
     *
     * <p>The constructor performs the following steps:
     * <ol>
     *   <li>Creates a DynamoDB client with the provided AWS credentials and region</li>
     *   <li>Validates that the specified DynamoDB table exists or creates it if it doesn't</li>
     *   <li>Configures the state backend with the client and table information</li>
     * </ol>
     *
     * @param configuration The configuration for the DynamoDB state backend, including:
     *                      <ul>
     *                        <li>AWS credentials and region settings</li>
     *                        <li>DynamoDB table name and capacity settings</li>
     *                        <li>Optional local DynamoDB settings for testing</li>
     *                      </ul>
     */
    public DynamoDbStateBackend(final Configuration configuration) {
        super(configuration);
    }


    /**
     * Creates a new DynamoDB state backend with the given client and configuration.
     *
     * <p>This constructor allows providing a pre-configured DynamoDB client, which can be useful
     * for testing or when special handling of DynamoDB operations is required. The constructor
     * will use the provided client to create or validate the DynamoDB table for state storage.
     *
     * <p>This constructor is particularly useful for:
     * <ul>
     *   <li>Unit testing with mock DynamoDB clients</li>
     *   <li>Integration testing with local DynamoDB instances</li>
     *   <li>Custom client configurations not supported by the standard configuration options</li>
     * </ul>
     *
     * @param client The pre-configured DynamoDB client for state operations
     * @param configuration The configuration for the DynamoDB state backend, including table settings
     */
    public DynamoDbStateBackend(final DynamoDbClient client, final Configuration configuration) {
        super(client, DynamoDbTableUtils.getOrCreateTable(client, configuration), new Configuration());
    }

    /**
     * Creates a new DynamoDB state backend with the given client and table.
     *
     * <p>This constructor allows providing a pre-configured DynamoDB client and table description,
     * which can be useful for testing or when special handling of DynamoDB operations is required.
     *
     * <p>This is the most direct constructor that bypasses table creation and validation,
     * assuming that the provided table description is valid and the table exists.
     *
     * <p>This constructor is particularly useful for:
     * <ul>
     *   <li>Unit testing with mock DynamoDB clients and tables</li>
     *   <li>Advanced scenarios where table creation and validation is handled externally</li>
     *   <li>Performance optimization by reusing existing client and table information</li>
     * </ul>
     *
     * @param client The pre-configured DynamoDB client for state operations
     * @param table The DynamoDB table description for an existing table
     */
    public DynamoDbStateBackend(
            final DynamoDbClient client,
            final TableDescription table) {
        super(client, table, new Configuration());
    }

    /**
     * Creates a DynamoDB client for the DynamoDB state backend.
     *
     * <p>This method creates a DynamoDB client using the provided configuration,
     * setting up the appropriate HTTP client, credentials, region, and user agent.
     *
     * <p>The client creation process includes:
     * <ol>
     *   <li>Creating a synchronous HTTP client with appropriate timeouts and connection settings</li>
     *   <li>Configuring AWS credentials based on the provided configuration</li>
     *   <li>Setting the AWS region for the client</li>
     *   <li>Configuring a custom user agent to identify requests from this state backend</li>
     *   <li>Applying any additional client configuration options</li>
     * </ol>
     *
     * <p>For local testing, the client can be configured to connect to a local DynamoDB
     * instance instead of the AWS service.
     *
     * @param configuration The configuration for creating the DynamoDB client, including
     *                      AWS credentials, region, and client settings
     * @return A fully configured DynamoDB client ready for use by the state backend
     */
    @Override
    protected DynamoDbClient createAwsClient(final Configuration configuration) {
        final Properties properties = new Properties();
        configuration.addAllToProperties(properties);

        final SdkHttpClient syncHttpClient = AWSClientUtil
                .createSyncHttpClient(properties, ApacheHttpClient.builder());

        return AWSClientUtil.createAwsSyncClient(
                properties,
                syncHttpClient,
                DynamoDbClient.builder(),
                DynamoDbStateBackendConstants.BASE_DDB_USER_AGENT_PREFIX_FORMAT,
                DynamoDbStateBackendConstants.DDB_CLIENT_USER_AGENT_PREFIX);
    }

    @Override
    protected TableDescription createAwsModel(final DynamoDbClient safeAwsClient,
                                              final Configuration configuration) {

        return DynamoDbTableUtils.getOrCreateTable(safeAwsClient, configuration);
    }

    @Override
    public <K> AbstractAwsKeyedStateBackendBuilder<K, DynamoDbClient, TableDescription, ?> createKeyedStateBackendBuilder(
            final DynamoDbClient safeAwsClient,
            final TableDescription safeAwsModel,
            final KeyedStateBackendParameters<K> parameters) {
        LOG.info(
                "Creating DynamoDB keyed state backend for operator {} in job {} for task {}",
                parameters.getOperatorIdentifier(),
                parameters.getJobID(),
                parameters.getEnv().getTaskInfo().getTaskNameWithSubtasks());

        return new DynamoDbKeyedStateBackendBuilder<>(
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
     * <p>This method creates a new DynamoDB state backend by merging the current configuration
     * with the provided configuration. This allows for overriding specific configuration options
     * while keeping the rest of the configuration intact.
     *
     * @param config      The configuration to merge with the current configuration.
     * @param classLoader The class loader to use for loading classes.
     * @return A new DynamoDB state backend with the merged configuration.
     */
    @Override
    public DynamoDbStateBackend configure(
            final ReadableConfig config,
            final ClassLoader classLoader) {
        final Configuration mergedConfig = new Configuration();
        mergedConfig.addAll(this.configuration);
        mergedConfig.addAll(Configuration.fromMap(config.toMap()));

        return new DynamoDbStateBackend(mergedConfig);
    }
}
