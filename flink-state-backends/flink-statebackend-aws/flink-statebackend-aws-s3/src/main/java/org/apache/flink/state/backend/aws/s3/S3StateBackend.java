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

package org.apache.flink.state.backend.aws.s3;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.backend.aws.AbstractAwsKeyedStateBackendBuilder;
import org.apache.flink.state.backend.aws.AbstractAwsStateBackend;
import org.apache.flink.state.backend.aws.s3.config.S3StateBackendConstants;
import org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions;
import org.apache.flink.state.backend.aws.util.AWSClientUtil;
import org.apache.flink.state.backend.aws.util.AWSGeneralUtil;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;

import java.util.Properties;

/**
 * State backend implementation that stores state in Amazon S3.
 *
 * <p>This state backend stores keyed state in S3 objects, using a structured key format
 * that encodes job, task, state name, key, and namespace information. It supports both
 * regular checkpoints and incremental checkpoints for efficient state management.
 *
 * <p>Key features:
 * <ul>
 *   <li>Durable state storage in S3 with configurable bucket and path settings</li>
 *   <li>Support for all standard Flink state types (ValueState, ListState, MapState, etc.)</li>
 *   <li>Automatic compression of state data using Snappy</li>
 *   <li>Optional encryption for sensitive state data</li>
 *   <li>Configurable S3 paths and prefixes for flexible organization</li>
 *   <li>TTL (Time-To-Live) support with automatic cleanup of expired state</li>
 *   <li>Incremental checkpointing support for efficient state snapshots</li>
 *   <li>AWS SDK v2.x with optimized client settings</li>
 * </ul>
 *
 * <p>The state backend requires configuration for:
 * <ul>
 *   <li>AWS credentials and region</li>
 *   <li>S3 bucket name</li>
 *   <li>Base path within the bucket</li>
 *   <li>Optional compression and encryption settings</li>
 * </ul>
 *
 * <p>Current limitations:
 * <ul>
 *   <li>Getting all keys for a specific namespace is not efficiently supported</li>
 *   <li>Priority queues are not yet supported</li>
 *   <li>Not recommended for production use yet</li>
 *   <li>Each state operation requires a network call to S3</li>
 *   <li>Some advanced features like incremental state visitors are not yet implemented</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Configuration config = new Configuration();
 * config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
 * config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
 * config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
 * config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");
 * config.set(S3StateBackendOptions.BUCKET_NAME, "flink-state");
 * config.set(S3StateBackendOptions.BASE_PATH, "my-job/state");
 * config.set(S3StateBackendOptions.STATE_FILE_COMPRESSION, true);
 *
 * S3StateBackend stateBackend = new S3StateBackend(config);
 * env.setStateBackend(stateBackend);
 * }</pre>
 *
 * <p>Alternatively, you can configure the state backend in your {@code flink-conf.yaml}:
 * <pre>{@code
 * state.backend: s3
 * state.backend.s3.bucket: flink-state
 * state.backend.s3.base-path: my-job/state
 * state.backend.s3.state-file-compression: true
 * aws.region: us-west-2
 * aws.credentials.provider: BASIC
 * aws.credentials.provider.basic.accesskeyid: your-access-key
 * aws.credentials.provider.basic.secretkey: your-secret-key
 * }</pre>
 *
 * @see org.apache.flink.state.backend.aws.config.AWSConfigOptions
 * @see org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions
 */
public class S3StateBackend extends AbstractAwsStateBackend<S3Client, Bucket> {

    private static final Logger LOG = LoggerFactory.getLogger(S3StateBackend.class);

    /**
     * Creates a new S3 state backend with the given configuration.
     *
     * <p>This constructor creates a new S3 client using the provided configuration
     * and initializes the state backend with the specified S3 bucket. The configuration
     * must include AWS credentials, region, and S3 bucket name.
     *
     * <p>The constructor performs the following steps:
     * <ol>
     *   <li>Creates an S3 client with the provided AWS credentials and region</li>
     *   <li>Validates that the specified S3 bucket exists and is accessible</li>
     *   <li>Configures the state backend with the client and bucket information</li>
     * </ol>
     *
     * @param configuration The configuration for the S3 state backend, including:
     *                      <ul>
     *                        <li>AWS credentials and region settings</li>
     *                        <li>S3 bucket name and path settings</li>
     *                        <li>Optional compression and encryption settings</li>
     *                      </ul>
     */
    public S3StateBackend(final Configuration configuration) {
        super(configuration);
    }

    /**
     * Creates a new S3 state backend with the given client, bucket, and configuration.
     *
     * <p>This constructor allows providing a pre-configured S3 client and bucket, which can be useful
     * for testing or when special handling of S3 operations is required.
     *
     * <p>This constructor is particularly useful for:
     * <ul>
     *   <li>Unit testing with mock S3 clients</li>
     *   <li>Integration testing with local S3-compatible services (like MinIO)</li>
     *   <li>Custom client configurations not supported by the standard configuration options</li>
     *   <li>Reusing existing S3 clients across multiple state backends</li>
     * </ul>
     *
     * @param s3Client The pre-configured S3 client for state operations
     * @param bucket The S3 bucket to use for state storage
     * @param configuration The configuration for the S3 state backend
     */
    public S3StateBackend(
            final S3Client s3Client,
            final Bucket bucket,
            final Configuration configuration) {
        super(s3Client, bucket, configuration);
    }

    /**
     * Creates an S3 client for the S3 state backend.
     *
     * <p>This method creates an S3 client using the provided configuration,
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
     * <p>For testing, the client can be configured to connect to a local S3-compatible
     * service like MinIO instead of the AWS S3 service by setting a custom endpoint.
     *
     * @param configuration The configuration for creating the S3 client, including
     *                      AWS credentials, region, and client settings
     * @return A fully configured S3 client ready for use by the state backend
     */
    @Override
    protected S3Client createAwsClient(final Configuration configuration) {
        final Properties properties = new Properties();
        configuration.addAllToProperties(properties);

        final SdkHttpClient syncHttpClient = AWSClientUtil
                .createSyncHttpClient(properties, ApacheHttpClient.builder());

        return AWSClientUtil.createAwsSyncClient(
                properties,
                syncHttpClient,
                S3Client.builder(),
                S3StateBackendConstants.BASE_S3_USER_AGENT_PREFIX_FORMAT,
                S3StateBackendConstants.S3_CLIENT_USER_AGENT_PREFIX);
    }

    /**
     * Creates an S3 bucket model from the configuration.
     *
     * <p>This method creates a bucket model object using the bucket name specified
     * in the configuration. The bucket model is used to identify the S3 bucket
     * where state data will be stored.
     *
     * <p>Note that this method only creates a model object and does not actually
     * create the bucket in S3. The bucket must already exist or will be created
     * when state operations are performed.
     *
     * @param safeAwsClient The S3 client to use for bucket operations
     * @param configuration The configuration containing the bucket name
     * @return A bucket model object representing the S3 bucket for state storage
     */
    @Override
    protected Bucket createAwsModel(final S3Client safeAwsClient, final Configuration configuration) {
        return Bucket.builder()
                .name(configuration.get(S3StateBackendOptions.BUCKET_NAME))
                .build();
    }

    @Override
    public <K> AbstractAwsKeyedStateBackendBuilder<K, S3Client, Bucket, S3KeyedStateBackend<K>> createKeyedStateBackendBuilder(
            final S3Client safeAwsClient,
            final Bucket safeAwsModel,
            final KeyedStateBackendParameters<K> parameters) {
        LOG.info(
                "Creating S3 keyed state backend for operator {} in job {}",
                parameters.getOperatorIdentifier(),
                parameters.getJobID());

        return new S3KeyedStateBackendBuilder<>(
                safeAwsClient,
                safeAwsModel,
                parameters.getEnv().getJobInfo(),
                parameters.getEnv().getTaskInfo(),
                parameters.getOperatorIdentifier(), configuration,
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
     * <p>This method creates a new S3 state backend by merging the current configuration
     * with the provided configuration. This allows for overriding specific configuration options
     * while keeping the rest of the configuration intact.
     *
     * @param config      The configuration to merge with the current configuration.
     * @param classLoader The class loader to use for loading classes.
     * @return A new S3 state backend with the merged configuration.
     */
    @Override
    public S3StateBackend configure(
            final ReadableConfig config,
            final ClassLoader classLoader) {
        final Configuration mergedConfig = new Configuration();
        mergedConfig.addAll(this.configuration);
        mergedConfig.addAll(Configuration.fromMap(config.toMap()));

        return new S3StateBackend(mergedConfig);
    }
}
