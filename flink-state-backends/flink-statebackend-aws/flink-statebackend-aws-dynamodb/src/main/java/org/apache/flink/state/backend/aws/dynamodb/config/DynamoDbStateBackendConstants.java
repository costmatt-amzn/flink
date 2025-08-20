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

package org.apache.flink.state.backend.aws.dynamodb.config;

/**
 * Constants used by the DynamoDB state backend.
 *
 * <p>This class defines configuration keys and other constants used throughout
 * the DynamoDB state backend implementation. These constants include:
 * <ul>
 *   <li>Configuration keys for table settings</li>
 *   <li>User agent information for AWS API requests</li>
 *   <li>Local testing options</li>
 * </ul>
 *
 * <p>These constants are used in conjunction with {@link DynamoDbStateBackendOptions}
 * to provide a consistent configuration interface for the DynamoDB state backend.
 *
 * <p>The configuration keys follow a consistent naming pattern:
 * <pre>state.backend.dynamodb.[category].[setting]</pre>
 * where:
 * <ul>
 *   <li><b>category</b>: The category of the setting (e.g., table, local)</li>
 *   <li><b>setting</b>: The specific setting name (e.g., name, rcu, wcu)</li>
 * </ul>
 */
public final class DynamoDbStateBackendConstants {

    /**
     * Format string for the DynamoDB client user agent prefix.
     *
     * <p>This format string is used to create a user agent prefix that identifies
     * requests coming from the Flink DynamoDB state backend. The format includes
     * the Flink version and runtime information.
     *
     * <p>The user agent prefix helps with:
     * <ul>
     *   <li>Request identification in AWS CloudTrail logs</li>
     *   <li>Usage tracking and analytics</li>
     *   <li>Troubleshooting API issues</li>
     * </ul>
     *
     * <p>Format parameters:
     * <ol>
     *   <li>Flink version</li>
     *   <li>Runtime information (e.g., Java version)</li>
     * </ol>
     */
    public static final String BASE_DDB_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) DynamoDb State Backend";

    /**
     * Configuration key for the DynamoDB Streams user agent prefix.
     *
     * <p>This key is used to set a custom user agent prefix for DynamoDB Streams clients.
     * The user agent helps identify and track API requests from the Flink state backend.
     */
    public static final String DDB_CLIENT_USER_AGENT_PREFIX =
            "aws.dynamodbstreams.client.user-agent-prefix";

    /**
     * Configuration key for the DynamoDB table name.
     *
     * <p>This key is used to specify the name of the DynamoDB table that will store
     * the state data. The table will be created automatically if it doesn't exist,
     * with the appropriate schema for storing Flink state data.
     *
     * <p>The table name must:
     * <ul>
     *   <li>Be between 3 and 255 characters long</li>
     *   <li>Contain only letters, numbers, underscores (_), hyphens (-), and dots (.)</li>
     *   <li>Be unique within an AWS region and account</li>
     * </ul>
     *
     * <p>This setting is required and has no default value.
     */
    public static final String TABLE_NAME_KEY = "state.backend.dynamodb.table.name";

    /**
     * Configuration key for the DynamoDB table read capacity units.
     *
     * <p>This key is used to specify the provisioned read capacity units for the DynamoDB table
     * when using provisioned capacity mode (i.e., when {@link #USE_ON_DEMAND_KEY} is set to false).
     *
     * <p>Higher values allow for more read operations per second but increase cost.
     *
     * <p>Default value: 10
     */
    public static final String READ_CAPACITY_UNITS_KEY = "state.backend.dynamodb.table.rcu";

    /**
     * Configuration key for the DynamoDB table write capacity units.
     *
     * <p>This key is used to specify the provisioned write capacity units for the DynamoDB table
     * when using provisioned capacity mode (i.e., when {@link #USE_ON_DEMAND_KEY} is set to false).
     *
     * <p>Higher values allow for more write operations per second but increase cost.
     *
     * <p>Default value: 10
     */
    public static final String WRITE_CAPACITY_UNITS_KEY = "state.backend.dynamodb.table.wcu";

    /**
     * Configuration key for using on-demand capacity mode for the DynamoDB table.
     *
     * <p>When set to true, the DynamoDB table will use on-demand capacity mode,
     * which automatically scales to accommodate your workload but may be more expensive
     * for predictable workloads. When false, the table will use provisioned capacity
     * mode with the specified read and write capacity units.
     *
     * <p>Default value: false
     */
    public static final String USE_ON_DEMAND_KEY = "state.backend.dynamodb.table.on-demand";

    /**
     * Configuration key for the path to the local DynamoDB database files.
     *
     * <p>This key is used to specify the file path for a local DynamoDB instance,
     * which can be used for testing without connecting to the AWS DynamoDB service.
     *
     * <p>This option is only used when testing with a local DynamoDB instance.
     */
    public static final String LOCAL_DB_PATH_KEY = "state.backend.dynamodb.table.local.path";

    /**
     * Configuration key for using in-memory mode for local DynamoDB.
     *
     * <p>When set to true, the state backend will use an in-memory local DynamoDB instance
     * instead of connecting to the AWS DynamoDB service. This is useful for testing
     * and development without incurring AWS costs.
     *
     * <p>Default value: false
     */
    public static final String LOCAL_IN_MEMORY_KEY = "state.backend.dynamodb.table.local.in-memory";

    /**
     * Separator used in composite keys for joining parts like job name, task name, etc.
     *
     * <p>This separator is used in various parts of the DynamoDB state backend to create
     * composite keys by joining multiple components. It is used in partition keys, sort keys,
     * and other attribute values to ensure consistent formatting across the codebase.
     */
    public static final String KEY_SEPARATOR = " -> ";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private DynamoDbStateBackendConstants() {
        // private constructor to prevent instantiation
    }
}
