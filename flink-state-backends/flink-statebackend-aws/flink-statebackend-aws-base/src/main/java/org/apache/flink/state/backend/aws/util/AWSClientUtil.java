/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.state.backend.aws.config.AWSConfigConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;

/**
 * Utility class for creating and configuring AWS clients using the AWS SDK for Java v2.
 *
 * <p>This class provides methods to create both synchronous and asynchronous AWS clients
 * with proper configuration for credentials, regions, endpoints, and user agents.
 * It extends {@link AWSGeneralUtil} to leverage common AWS configuration functionality.
 *
 * <p>The utility methods in this class handle the complexities of AWS client configuration,
 * including:
 * <ul>
 *   <li>Setting up credentials providers based on configuration</li>
 *   <li>Configuring regions and endpoints</li>
 *   <li>Setting appropriate user agent strings</li>
 *   <li>Configuring HTTP clients and timeouts</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Properties configProps = new Properties();
 * configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-west-2");
 * configProps.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
 * configProps.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "your-access-key");
 * configProps.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "your-secret-key");
 *
 * SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder().build();
 * DynamoDbAsyncClient dynamoClient = AWSClientUtil.createAwsAsyncClient(
 *     configProps,
 *     httpClient,
 *     DynamoDbAsyncClient.builder(),
 *     "Flink-DynamoDB-Client/%s-%s",
 *     "aws.dynamodb.client.user-agent-prefix");
 * }</pre>
 */
@Internal
public class AWSClientUtil extends AWSGeneralUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AWSClientUtil.class);

    /**
     * V2 suffix to denote clients using AWS SDK v2.
     * This distinguishes from V1 clients that might use different implementations like KPL.
     */
    static final String V2_USER_AGENT_SUFFIX = " V2";

    /**
     * Creates a user agent prefix for Flink. This can be used by HTTP Clients.
     *
     * <p>The user agent format typically includes placeholders for version and commit ID,
     * which are filled with the current Flink version and revision information.
     *
     * @param userAgentFormat flink user agent prefix format with placeholders for version and
     *     commit id.
     * @return a user agent prefix for Flink
     */
    public static String formatFlinkUserAgentPrefix(final String userAgentFormat) {
        final String version = EnvironmentInformation.getVersion();
        final String commitId = EnvironmentInformation.getRevisionInformation().commitId;
        LOG.trace("Formatting user agent with Flink version: {} and commit ID: {}", version, commitId);

        final String formattedUserAgent = String.format(userAgentFormat, version, commitId);
        LOG.trace("Formatted user agent: {}", formattedUserAgent);
        return formattedUserAgent;
    }

    /**
     * Creates an AWS Async Client with default client configuration.
     *
     * <p>This method creates an asynchronous AWS client using the provided configuration properties,
     * HTTP client, and client builder. It configures the client with appropriate credentials,
     * region, endpoint, and user agent information.
     *
     * @param configProps configuration properties containing AWS settings
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @param clientBuilder the builder for the specific AWS service client
     * @param awsUserAgentPrefixFormat format string for the user agent prefix
     * @param awsClientUserAgentPrefix property key for custom user agent prefix
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     * @return a new configured AWS Async Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final SdkAsyncHttpClient httpClient,
                    final T clientBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        LOG.debug("Creating AWS async client with user agent prefix format: {}", awsUserAgentPrefixFormat);
        final SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();
        return createAwsAsyncClient(
                configProps,
                clientConfiguration,
                httpClient,
                clientBuilder,
                awsUserAgentPrefixFormat,
                awsClientUserAgentPrefix);
    }

    /**
     * Creates an AWS Async Client with custom client configuration.
     *
     * <p>This method creates an asynchronous AWS client using the provided configuration properties,
     * client configuration, HTTP client, and client builder. It allows for more detailed configuration
     * of the AWS client through the provided client configuration.
     *
     * @param configProps configuration properties containing AWS settings
     * @param clientConfiguration the AWS SDK v2 config to instantiate the client
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @param clientBuilder httpClientBuilder to build the underlying HTTP client
     * @param awsUserAgentPrefixFormat user agent prefix format for Flink
     * @param awsClientUserAgentPrefix property key for custom user agent prefix
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     * @return a new configured AWS Async Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final SdkClientConfiguration clientConfiguration,
                    final SdkAsyncHttpClient httpClient,
                    final T clientBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        LOG.debug("Creating AWS async client with custom configuration");

        final String flinkUserAgentPrefix =
                getFlinkUserAgentPrefix(
                        configProps, awsUserAgentPrefixFormat, awsClientUserAgentPrefix);

        LOG.trace("Using Flink user agent prefix: {}", flinkUserAgentPrefix);

        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(
                        clientConfiguration,
                        ClientOverrideConfiguration.builder(),
                        flinkUserAgentPrefix);

        LOG.trace("Client override configuration created");

        final S client = createAwsAsyncClient(configProps, clientBuilder, httpClient, overrideConfiguration);
        LOG.debug("AWS async client created successfully: {}", client.getClass().getSimpleName());
        return client;
    }

    /**
     * Creates a client override configuration with the specified user agent prefix.
     *
     * <p>This method configures client override settings, including user agent information
     * and timeout settings from the provided client configuration.
     *
     * @param config the client configuration containing settings to apply
     * @param overrideConfigurationBuilder the builder for client override configuration
     * @param flinkUserAgentPrefix the user agent prefix to use
     * @return a configured client override configuration
     */
    @VisibleForTesting
    static ClientOverrideConfiguration createClientOverrideConfiguration(
            final SdkClientConfiguration config,
            final ClientOverrideConfiguration.Builder overrideConfigurationBuilder,
            final String flinkUserAgentPrefix) {

        LOG.debug("Creating client override configuration with user agent prefix: {}", flinkUserAgentPrefix);

        overrideConfigurationBuilder
                .putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX, flinkUserAgentPrefix)
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                        config.option(SdkAdvancedClientOption.USER_AGENT_SUFFIX));

        LOG.trace("User agent options configured");

        if (config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT) != null) {
            LOG.trace("Setting API call attempt timeout: {}", config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT));
            overrideConfigurationBuilder.apiCallAttemptTimeout(config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT));
        }

        if (config.option(SdkClientOption.API_CALL_TIMEOUT) != null) {
            LOG.trace("Setting API call timeout: {}", config.option(SdkClientOption.API_CALL_TIMEOUT));
            overrideConfigurationBuilder.apiCallTimeout(config.option(SdkClientOption.API_CALL_TIMEOUT));
        }

        final ClientOverrideConfiguration overrideConfig = overrideConfigurationBuilder.build();
        LOG.debug("Client override configuration created successfully");
        return overrideConfig;
    }

    /**
     * Creates an AWS Async Client with the specified configuration.
     *
     * <p>This method applies the final configuration to the client builder and builds the client.
     *
     * @param configProps configuration properties containing AWS settings
     * @param clientBuilder the builder for the AWS service client
     * @param httpClient the HTTP client to use
     * @param overrideConfiguration the client override configuration
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     * @return a new configured AWS Async Client
     */
    @VisibleForTesting
    static <
                    S extends SdkClient,
                    T extends
                            AwsAsyncClientBuilder<? extends T, S>
                                    & AwsClientBuilder<? extends T, S>>
            S createAwsAsyncClient(
                    final Properties configProps,
                    final T clientBuilder,
                    final SdkAsyncHttpClient httpClient,
                    final ClientOverrideConfiguration overrideConfiguration) {

        LOG.trace("Configuring AWS async client with properties");
        updateEndpointOverride(configProps, clientBuilder);

        try {
            LOG.trace("Building AWS async client with region: {}", getRegion(configProps));
            final S client = clientBuilder
                    .httpClient(httpClient)
                    .overrideConfiguration(overrideConfiguration)
                    .credentialsProvider(getCredentialsProvider(configProps))
                    .region(getRegion(configProps))
                    .build();
            LOG.trace("AWS async client built successfully");
            return client;
        } catch (final Exception e) {
            LOG.error("Failed to build AWS async client", e);
            throw e;
        }
    }

    /**
     * Creates an AWS Sync Client with default override configuration.
     *
     * <p>This method creates a synchronous AWS client using the provided configuration properties,
     * HTTP client, and client builder. It configures the client with appropriate credentials,
     * region, endpoint, and user agent information.
     *
     * @param configProps configuration properties containing AWS settings
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @param clientBuilder the builder for the AWS SDK client
     * @param awsUserAgentPrefixFormat user agent prefix format for Flink
     * @param awsClientUserAgentPrefix user agent prefix for the AWS client
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     * @return a new configured AWS Sync Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsSyncClientBuilder<? extends T, S> & AwsClientBuilder<? extends T, S>>
            S createAwsSyncClient(
                    final Properties configProps,
                    final SdkHttpClient httpClient,
                    final T clientBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        LOG.debug("Creating AWS sync client with user agent prefix format: {}", awsUserAgentPrefixFormat);
        return createAwsSyncClient(
                configProps,
                httpClient,
                clientBuilder,
                ClientOverrideConfiguration.builder(),
                awsUserAgentPrefixFormat,
                awsClientUserAgentPrefix);
    }

    /**
     * Creates an AWS Sync Client with custom override configuration.
     *
     * <p>This method creates a synchronous AWS client using the provided configuration properties,
     * HTTP client, client builder, and override configuration builder. It allows for more detailed
     * configuration of the AWS client through the provided override configuration builder.
     *
     * @param configProps configuration properties containing AWS settings
     * @param httpClient the underlying HTTP client used to talk to AWS
     * @param clientBuilder the builder for the AWS SDK client
     * @param clientOverrideConfigurationBuilder the builder for custom override configuration
     * @param awsUserAgentPrefixFormat user agent prefix format for Flink
     * @param awsClientUserAgentPrefix user agent prefix for the AWS client
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     * @return a new configured AWS Sync Client
     */
    public static <
                    S extends SdkClient,
                    T extends
                            AwsSyncClientBuilder<? extends T, S> & AwsClientBuilder<? extends T, S>>
            S createAwsSyncClient(
                    final Properties configProps,
                    final SdkHttpClient httpClient,
                    final T clientBuilder,
                    final ClientOverrideConfiguration.Builder clientOverrideConfigurationBuilder,
                    final String awsUserAgentPrefixFormat,
                    final String awsClientUserAgentPrefix) {
        LOG.debug("Creating AWS sync client with custom configuration");

        final SdkClientConfiguration clientConfiguration = SdkClientConfiguration.builder().build();

        final String flinkUserAgentPrefix =
                getFlinkUserAgentPrefix(
                        configProps, awsUserAgentPrefixFormat, awsClientUserAgentPrefix);

        LOG.trace("Using Flink user agent prefix: {}", flinkUserAgentPrefix);

        final ClientOverrideConfiguration overrideConfiguration =
                createClientOverrideConfiguration(
                        clientConfiguration,
                        clientOverrideConfigurationBuilder,
                        flinkUserAgentPrefix);

        LOG.trace("Client override configuration created");

        updateEndpointOverride(configProps, clientBuilder);

        try {
            LOG.trace("Building AWS sync client with region: {}", getRegion(configProps));
            final S client = clientBuilder
                    .httpClient(httpClient)
                    .overrideConfiguration(overrideConfiguration)
                    .credentialsProvider(getCredentialsProvider(configProps))
                    .region(getRegion(configProps))
                    .build();
            LOG.debug("AWS sync client created successfully: {}", client.getClass().getSimpleName());
            return client;
        } catch (final Exception e) {
            LOG.error("Failed to build AWS sync client", e);
            throw e;
        }
    }

    /**
     * Gets the Flink user agent prefix from configuration or creates a default one.
     *
     * <p>This method first checks if a custom user agent prefix is specified in the configuration.
     * If not, it creates a default one using the provided format string.
     *
     * @param configProps configuration properties containing AWS settings
     * @param awsUserAgentPrefixFormat format string for the user agent prefix
     * @param awsClientUserAgentPrefix property key for custom user agent prefix
     * @return the Flink user agent prefix to use
     */
    private static String getFlinkUserAgentPrefix(
            final Properties configProps,
            final String awsUserAgentPrefixFormat,
            final String awsClientUserAgentPrefix) {

        final String userAgentPrefix = configProps.getProperty(awsClientUserAgentPrefix);
        if (userAgentPrefix != null) {
            LOG.trace("Using custom user agent prefix from property {}: {}",
                    awsClientUserAgentPrefix, userAgentPrefix);
            return userAgentPrefix;
        } else {
            final String defaultPrefix = formatFlinkUserAgentPrefix(awsUserAgentPrefixFormat + V2_USER_AGENT_SUFFIX);
            LOG.trace("Using default user agent prefix: {}", defaultPrefix);
            return defaultPrefix;
        }
    }

    /**
     * Updates the endpoint override for an AWS client builder if specified in the configuration.
     *
     * <p>This method checks if a custom endpoint is specified in the configuration and
     * sets it on the client builder if present.
     *
     * @param configProps configuration properties containing AWS settings
     * @param clientBuilder the builder for the AWS client
     * @param <S> the type of the AWS service client
     * @param <T> the type of the AWS client builder
     */
    private static <S extends SdkClient, T extends AwsClientBuilder<? extends T, S>>
            void updateEndpointOverride(final Properties configProps, final T clientBuilder) {
        if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            final String endpoint = configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT);
            LOG.debug("Using custom AWS endpoint: {}", endpoint);
            final URI endpointOverride = URI.create(endpoint);
            clientBuilder.endpointOverride(endpointOverride);
        } else {
            LOG.trace("Using default AWS endpoint");
        }
    }
}
