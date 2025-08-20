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

package org.apache.flink.state.backend.aws.dynamodb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.state.backend.aws.AbstractAwsStateBackendFactory;
import org.apache.flink.state.backend.aws.config.AWSConfigOptions;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendOptions;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Factory for creating {@link DynamoDbStateBackend} instances.
 *
 * <p>This factory implements the Flink {@link StateBackendFactory} interface to allow
 * the DynamoDB state backend to be instantiated from configuration. It validates that
 * required configuration parameters are present and creates a properly configured
 * {@link DynamoDbStateBackend} instance.
 *
 * <p>Required configuration parameters:
 * <ul>
 *   <li>{@link AWSConfigOptions#AWS_REGION_OPTION} - The AWS region where the DynamoDB table is located</li>
 *   <li>{@link DynamoDbStateBackendOptions#TABLE_NAME} - The name of the DynamoDB table to use for state storage</li>
 * </ul>
 *
 * <p>This factory can be used in Flink configuration by setting:
 * <pre>{@code
 * state.backend: dynamodb
 * }</pre>
 *
 * @see DynamoDbStateBackend
 * @see AWSConfigOptions
 * @see DynamoDbStateBackendOptions
 */
public class DynamoDbStateBackendFactory extends AbstractAwsStateBackendFactory<DynamoDbStateBackend, DynamoDbClient, TableDescription> {

    @Override
    protected void validateConfiguration(final Configuration configuration) throws IllegalArgumentException {
        // Validate required configuration
        if (configuration.get(DynamoDbStateBackendOptions.TABLE_NAME) == null) {
            throw new IllegalArgumentException(
                    "Missing required configuration for DynamoDB state backend. " +
                            "Please specify " + DynamoDbStateBackendOptions.TABLE_NAME.key());
        }
    }

    @Override
    protected DynamoDbStateBackend createStateBackend(final Configuration configuration) {
        return new DynamoDbStateBackend(configuration);
    }

    @Override
    public String toString() {
        return "S3StateBackendFactory";
    }
}
