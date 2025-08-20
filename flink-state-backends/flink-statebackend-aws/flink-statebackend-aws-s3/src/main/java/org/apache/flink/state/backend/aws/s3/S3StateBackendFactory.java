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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.backend.aws.AbstractAwsStateBackendFactory;
import org.apache.flink.state.backend.aws.config.AWSConfigOptions;
import org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;

/**
 * Factory for creating {@link S3StateBackend} instances.
 *
 * <p>This factory implements the Flink {@link org.apache.flink.runtime.state.StateBackendFactory} interface to allow
 * the S3 state backend to be instantiated from configuration. It creates a properly configured
 * {@link S3StateBackend} instance using the provided configuration.
 *
 * <p>Required configuration parameters:
 * <ul>
 *   <li>{@link org.apache.flink.state.backend.aws.config.AWSConfigOptions#AWS_REGION_OPTION} - The AWS region where the S3 bucket is located</li>
 *   <li>{@link org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions#BUCKET_NAME} - The name of the S3 bucket to use for state storage</li>
 * </ul>
 *
 * <p>This factory can be used in Flink configuration by setting:
 * <pre>{@code
 * state.backend: s3
 * }</pre>
 *
 * @see S3StateBackend
 * @see org.apache.flink.state.backend.aws.config.AWSConfigOptions
 * @see org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions
 */
public class S3StateBackendFactory extends AbstractAwsStateBackendFactory<S3StateBackend, S3Client, Bucket> {

    @Override
    protected void validateConfiguration(final Configuration configuration) throws IllegalArgumentException {
        // Validate required configuration
        if (configuration.get(AWSConfigOptions.AWS_REGION_OPTION) == null ||
                configuration.get(S3StateBackendOptions.BUCKET_NAME) == null) {
            throw new IllegalArgumentException(
                    "Missing required configuration for S3 state backend. " +
                    "Please specify " + AWSConfigOptions.AWS_REGION_OPTION.key() + " and " +
                    S3StateBackendOptions.BUCKET_NAME.key());
        }
    }

    @Override
    protected S3StateBackend createStateBackend(final Configuration configuration) {
        return new S3StateBackend(configuration);
    }

    @Override
    public String toString() {
        return "S3StateBackendFactory";
    }
}
