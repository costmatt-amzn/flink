/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License att
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.dynamodb.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.LOCAL_DB_PATH_KEY;
import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.LOCAL_IN_MEMORY_KEY;
import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.READ_CAPACITY_UNITS_KEY;
import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.TABLE_NAME_KEY;
import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.USE_ON_DEMAND_KEY;
import static org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendConstants.WRITE_CAPACITY_UNITS_KEY;

/**
 * Configuration options specific to the DynamoDB state backend.
 *
 * <p>These options control the behavior of the DynamoDB state backend, including
 * table name, capacity settings, and local testing options.
 *
 * <p>Key configuration areas:
 * <ul>
 *   <li><b>Table Configuration</b>: Table name and creation settings</li>
 *   <li><b>Capacity Management</b>: Provisioned or on-demand capacity settings</li>
 *   <li><b>Local Testing</b>: Options for using local DynamoDB for development</li>
 * </ul>
 *
 * <p>These options can be set in the Flink configuration file (flink-conf.yaml):
 * <pre>
 * state.backend: dynamodb
 * state.backend.dynamodb.table: flink-state
 * state.backend.dynamodb.use-on-demand: true
 * </pre>
 *
 * <p>Or programmatically when creating the DynamoDB state backend:
 * <pre>{@code
 * Configuration config = new Configuration();
 * config.set(DynamoDbStateBackendOptions.TABLE_NAME, "flink-state");
 * config.set(DynamoDbStateBackendOptions.USE_ON_DEMAND, true);
 * DynamoDbStateBackend stateBackend = new DynamoDbStateBackend(config);
 * env.setStateBackend(stateBackend);
 * }</pre>
 */
public class DynamoDbStateBackendOptions {

    /**
     * The DynamoDB table name.
     *
     * <p>This option specifies the name of the DynamoDB table to use for state storage.
     * The table will be created automatically if it doesn't exist, with the appropriate
     * schema for storing Flink state data.
     *
     * <p>Table schema:
     * <ul>
     *   <li>Partition key: {@link DynamoDbAttributes#JOB_AWARE_STATE_DESC_ATTR} (String)</li>
     *   <li>Sort key: {@link DynamoDbAttributes#KEY_AND_NAMESPACE_ATTR} (String)</li>
     *   <li>Value: {@link DynamoDbAttributes#VALUE_ATTR} (Binary)</li>
     *   <li>TTL: {@link DynamoDbAttributes#TTL_ATTR} (Number)</li>
     *   <li>Additional metadata attributes</li>
     * </ul>
     *
     * <p>This option is required and has no default value.
     */
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key(TABLE_NAME_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("The DynamoDB table name.");

    /**
     * The read capacity units for DynamoDB tables.
     *
     * <p>This option specifies the provisioned read capacity units for the DynamoDB table
     * when using provisioned capacity mode (i.e., when {@link #USE_ON_DEMAND} is false).
     *
     * <p>Read capacity units represent the number of strongly consistent reads per second
     * for items up to 4 KB in size. For example, with 10 RCUs, you can perform:
     * <ul>
     *   <li>10 strongly consistent reads per second for items up to 4 KB</li>
     *   <li>20 eventually consistent reads per second for items up to 4 KB</li>
     *   <li>5 strongly consistent reads per second for items up to 8 KB</li>
     * </ul>
     *
     * <p>Higher values allow for more read operations per second but increase cost.
     * Consider your application's read patterns and adjust this value accordingly.
     *
     * <p>Default value: 10
     */
    public static final ConfigOption<Integer> READ_CAPACITY_UNITS = ConfigOptions
            .key(READ_CAPACITY_UNITS_KEY)
            .intType()
            .defaultValue(10)
            .withDescription("The read capacity units for DynamoDB tables.");

    /**
     * The write capacity units for DynamoDB tables.
     *
     * <p>This option specifies the provisioned write capacity units for the DynamoDB table
     * when using provisioned capacity mode (i.e., when {@link #USE_ON_DEMAND} is false).
     *
     * <p>Write capacity units represent the number of write operations per second
     * for items up to 1 KB in size. For example, with 10 WCUs, you can perform:
     * <ul>
     *   <li>10 standard write operations per second for items up to 1 KB</li>
     *   <li>5 standard write operations per second for items up to 2 KB</li>
     * </ul>
     *
     * <p>Higher values allow for more write operations per second but increase cost.
     * Consider your application's write patterns and adjust this value accordingly.
     *
     * <p>Default value: 10
     */
    public static final ConfigOption<Integer> WRITE_CAPACITY_UNITS = ConfigOptions
            .key(WRITE_CAPACITY_UNITS_KEY)
            .intType()
            .defaultValue(10)
            .withDescription("The write capacity units for DynamoDB tables.");

    /**
     * Whether to use on-demand capacity mode for DynamoDB tables.
     *
     * <p>When set to true, the DynamoDB table will use on-demand capacity mode,
     * which automatically scales to accommodate your workload without requiring
     * capacity planning. You pay per request rather than for provisioned capacity.
     *
     * <p>On-demand capacity mode is recommended for:
     * <ul>
     *   <li>New applications with unknown workloads</li>
     *   <li>Applications with highly variable or unpredictable workloads</li>
     *   <li>Development and testing environments</li>
     * </ul>
     *
     * <p>When false, the table will use provisioned capacity mode with the specified
     * read and write capacity units, which may be more cost-effective for predictable workloads.
     *
     * <p>Default value: false
     */
    public static final ConfigOption<Boolean> USE_ON_DEMAND = ConfigOptions
            .key(USE_ON_DEMAND_KEY)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use on-demand capacity mode for DynamoDB tables.");

    /**
     * The path to the local DynamoDB tables for local storage.
     *
     * <p>This option specifies the file path for a local DynamoDB instance,
     * which can be used for testing without connecting to the AWS DynamoDB service.
     * The local DynamoDB instance will store its data files in this location.
     *
     * <p>This option is only used when testing with a local DynamoDB instance and
     * is typically used in development environments to avoid AWS costs.
     *
     * <p>Example: "/tmp/dynamodb-local"
     *
     * <p>Note: This option is mutually exclusive with {@link #LOCAL_IN_MEMORY}.
     * If both are specified, {@link #LOCAL_IN_MEMORY} takes precedence.
     */
    public static final ConfigOption<String> LOCAL_DB_PATH = ConfigOptions
            .key(LOCAL_DB_PATH_KEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Whether to use local in-memory DynamoDB tables for local storage.");

    /**
     * Whether to use local in-memory DynamoDB tables for local storage.
     *
     * <p>When set to true, the state backend will use an in-memory local DynamoDB instance
     * instead of connecting to the AWS DynamoDB service. This is useful for testing
     * and development without incurring AWS costs.
     *
     * <p>The in-memory DynamoDB instance is ephemeral and will not persist data
     * between application restarts. All data is lost when the application terminates.
     *
     * <p>This option is particularly useful for:
     * <ul>
     *   <li>Unit testing</li>
     *   <li>Integration testing</li>
     *   <li>Local development</li>
     *   <li>CI/CD pipelines</li>
     * </ul>
     *
     * <p>Note: This option takes precedence over {@link #LOCAL_DB_PATH} if both are specified.
     *
     * <p>Default value: false
     */
    public static final ConfigOption<Boolean> LOCAL_IN_MEMORY = ConfigOptions
            .key(LOCAL_IN_MEMORY_KEY)
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use local in-memory DynamoDB tables for local storage.");

}
