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

package org.apache.flink.state.backend.aws.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.accessKeyId;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.customCredentialsProviderClass;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.externalId;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.profileName;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.profilePath;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.roleArn;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.roleSessionName;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.roleStsEndpoint;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.secretKey;
import static org.apache.flink.state.backend.aws.config.AWSConfigConstants.webIdentityTokenFile;


/**
 * Configuration options for AWS service integration.
 *
 * <p>This class provides configuration options for AWS credentials, region settings,
 * endpoints, and retry strategies used by AWS state backends.
 *
 * <p>The options defined here can be used in the Flink configuration or directly
 * when creating state backends programmatically.
 *
 * <p>Key configuration categories:
 * <ul>
 *   <li>AWS Credentials: Various credential provider options including basic credentials,
 *       environment variables, profiles, and role-based authentication</li>
 *   <li>AWS Region and Endpoints: Region selection and custom endpoint configuration</li>
 *   <li>HTTP Client Settings: Protocol version, timeouts, and concurrency settings</li>
 *   <li>Retry Strategy: Configurable exponential backoff with customizable delays and attempts</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Configuration config = new Configuration();
 * // Basic AWS configuration
 * config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
 * config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
 * config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
 * config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");
 *
 * // Advanced settings
 * config.set(AWSConfigOptions.RETRY_STRATEGY_MAX_ATTEMPTS_OPTION, 10);
 * config.set(AWSConfigOptions.HTTP_CLIENT_MAX_CONCURRENCY_OPTION, "100");
 * }</pre>
 *
 * <p>Or in your {@code flink-conf.yaml}:
 * <pre>
 * aws.region: us-west-2
 * aws.credentials.provider: BASIC
 * aws.credentials.provider.basic.accesskeyid: your-access-key
 * aws.credentials.provider.basic.secretkey: your-secret-key
 * retry-strategy.attempts.max: 10
 * </pre>
 *
 * @see org.apache.flink.state.backend.aws.config.AWSConfigConstants
 */
@PublicEvolving
public class AWSConfigOptions {
    /**
     * The AWS region to use for AWS services.
     *
     * <p>This option specifies the AWS region where the AWS services will be accessed.
     * The region affects the endpoints used for AWS service calls.
     *
     * <p>This option is required and has no default value.
     */
    public static final ConfigOption<String> AWS_REGION_OPTION =
            ConfigOptions.key(AWS_REGION)
                    .stringType()
                    .defaultValue("us-east-1")
                    .withDescription(
                            "The AWS region of the service (\"us-east-1\" is used if not set).");

    /**
     * The credential provider type to use for AWS authentication.
     *
     * <p>This option specifies how AWS credentials are obtained. The following provider types are supported:
     * <ul>
     *   <li>{@link AWSConfigConstants.CredentialProvider#BASIC}: Use access key ID and secret key</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#ENV_VAR}: Use environment variables</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#SYS_PROP}: Use Java system properties</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#PROFILE}: Use AWS credentials profile file</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#ASSUME_ROLE}: Assume an IAM role</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#WEB_IDENTITY_TOKEN}: Use AWS Web Identity Token</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#CUSTOM}: Use a custom credentials provider class</li>
     *   <li>{@link AWSConfigConstants.CredentialProvider#AUTO}: Use the default AWS credentials provider chain</li>
     * </ul>
     *
     * <p>Default value: {@link AWSConfigConstants.CredentialProvider#BASIC}
     */
    public static final ConfigOption<AWSConfigConstants.CredentialProvider>
            AWS_CREDENTIALS_PROVIDER_OPTION =
                    ConfigOptions.key(AWS_CREDENTIALS_PROVIDER)
                            .enumType(AWSConfigConstants.CredentialProvider.class)
                            .defaultValue(AWSConfigConstants.CredentialProvider.BASIC)
                            .withDescription(
                                    "The credential provider type to use when AWS credentials are required (BASIC is used if not set");

    /**
     * The AWS access key ID to use when setting credentials provider type to BASIC.
     *
     * <p>This option specifies the AWS access key ID for the BASIC credentials provider.
     * It is required when using the BASIC credentials provider.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ACCESS_KEY_ID_OPTION =
            ConfigOptions.key(accessKeyId(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS access key ID to use when setting credentials provider type to BASIC.");

    /**
     * The AWS secret key to use when setting credentials provider type to BASIC.
     *
     * <p>This option specifies the AWS secret key for the BASIC credentials provider.
     * It is required when using the BASIC credentials provider.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_SECRET_ACCESS_KEY_OPTION =
            ConfigOptions.key(secretKey(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS secret key to use when setting credentials provider type to BASIC.");

    /**
     * Optional configuration for profile path if credential provider type is set to be PROFILE.
     *
     * <p>This option specifies the path to the AWS credentials profile file.
     * It is used when the credentials provider type is set to PROFILE.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_PROFILE_PATH_OPTION =
            ConfigOptions.key(profilePath(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional configuration for profile path if credential provider type is set to be PROFILE.");

    /**
     * Optional configuration for profile name if credential provider type is set to be PROFILE.
     *
     * <p>This option specifies the name of the profile to use from the AWS credentials profile file.
     * It is used when the credentials provider type is set to PROFILE.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_PROFILE_NAME_OPTION =
            ConfigOptions.key(profileName(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional configuration for profile name if credential provider type is set to be PROFILE.");

    /**
     * The AWS endpoint for the STS to use if credential provider type is set to be ASSUME_ROLE.
     *
     * <p>This option specifies the endpoint URL for the AWS Security Token Service (STS).
     * It is used when the credentials provider type is set to ASSUME_ROLE.
     * If not set, the endpoint will be derived from the AWS region setting.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ROLE_STS_ENDPOINT_OPTION =
            ConfigOptions.key(roleStsEndpoint(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS endpoint for the STS (derived from the AWS region setting if not set) "
                                    + "to use if credential provider type is set to be ASSUME_ROLE.");

    /**
     * The full path to the user provided class to use if credential provider type is set to be CUSTOM.
     *
     * <p>This option specifies the fully qualified class name of a custom AWS credentials provider.
     * It is used when the credentials provider type is set to CUSTOM.
     * The class must implement {@link software.amazon.awssdk.auth.credentials.AwsCredentialsProvider} and have a no-argument constructor.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> CUSTOM_CREDENTIALS_PROVIDER_CLASS_OPTION =
            ConfigOptions.key(customCredentialsProviderClass(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The full path (e.g. org.user_company.auth.CustomAwsCredentialsProvider) to the user provided"
                                    + "class to use if credential provider type is set to be CUSTOM.");

    /**
     * The role ARN to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.
     *
     * <p>This option specifies the Amazon Resource Name (ARN) of the role to assume.
     * It is used when the credentials provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ROLE_ARN_OPTION =
            ConfigOptions.key(roleArn(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The role ARN to use when credential provider type is set to ASSUME_ROLE or"
                                    + "WEB_IDENTITY_TOKEN");

    /**
     * The role session name to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.
     *
     * <p>This option specifies the name of the session when assuming a role.
     * It is used when the credentials provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ROLE_SESSION_NAME =
            ConfigOptions.key(roleSessionName(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The role session name to use when credential provider type is set to ASSUME_ROLE or"
                                    + "WEB_IDENTITY_TOKEN");

    /**
     * The external ID to use when credential provider type is set to ASSUME_ROLE.
     *
     * <p>This option specifies the external ID to use when assuming a role.
     * It is used when the credentials provider type is set to ASSUME_ROLE.
     * The external ID is a unique identifier that might be required when you assume a role in another account.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ROLE_EXTERNAL_ID_OPTION =
            ConfigOptions.key(externalId(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The external ID to use when credential provider type is set to ASSUME_ROLE.");

    /**
     * The absolute path to the web identity token file that should be used if provider type is set to WEB_IDENTITY_TOKEN.
     *
     * <p>This option specifies the path to the web identity token file.
     * It is used when the credentials provider type is set to WEB_IDENTITY_TOKEN.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_WEB_IDENTITY_TOKEN_FILE =
            ConfigOptions.key(webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The absolute path to the web identity token file that should be used if provider"
                                    + " type is set to WEB_IDENTITY_TOKEN.");

    /**
     * The credentials provider that provides credentials for assuming the role when credential provider type is set to ASSUME_ROLE.
     *
     * <p>This option specifies the credential provider to use for assuming a role.
     * It is used when the credentials provider type is set to ASSUME_ROLE.
     * Roles can be nested, so AWS_ROLE_CREDENTIALS_PROVIDER can again be set to ASSUME_ROLE.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ROLE_CREDENTIALS_PROVIDER_OPTION =
            ConfigOptions.key(webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER))
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The credentials provider that provides credentials for assuming the role when"
                                    + " credential provider type is set to ASSUME_ROLE. Roles can be nested, so"
                                    + " AWS_ROLE_CREDENTIALS_PROVIDER can again be set to ASSUME_ROLE");

    /**
     * The AWS endpoint for the service.
     *
     * <p>This option specifies the endpoint URL for the AWS service.
     * If not set, the endpoint will be derived from the AWS region setting.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> AWS_ENDPOINT_OPTION =
            ConfigOptions.key(AWSConfigConstants.AWS_ENDPOINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The AWS endpoint for the service (derived from the AWS region setting if not set).");

    /**
     * Whether to trust all SSL certificates.
     *
     * <p>This option specifies whether to trust all SSL certificates, including self-signed certificates.
     * This should only be set to true in development or testing environments.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> TRUST_ALL_CERTIFICATES_OPTION =
            ConfigOptions.key(AWSConfigConstants.TRUST_ALL_CERTIFICATES)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Whether to trust all SSL certificates.");

    /**
     * The HTTP protocol version to use.
     *
     * <p>This option specifies the HTTP protocol version to use for AWS service requests.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> HTTP_PROTOCOL_VERSION_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_PROTOCOL_VERSION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The HTTP protocol version to use.");

    /**
     * Maximum request concurrency for SdkAsyncHttpClient.
     *
     * <p>This option specifies the maximum number of concurrent requests for the AWS SDK async HTTP client.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> HTTP_CLIENT_MAX_CONCURRENCY_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Maximum request concurrency for SdkAsyncHttpClient.");

    /**
     * Read Request timeout for SdkAsyncHttpClient.
     *
     * <p>This option specifies the read timeout in milliseconds for the AWS SDK async HTTP client.
     *
     * <p>This option has no default value.
     */
    public static final ConfigOption<String> HTTP_CLIENT_READ_TIMEOUT_MILLIS_OPTION =
            ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Read Request timeout for SdkAsyncHttpClient.");

    /**
     * Base delay for the exponential backoff retry strategy.
     *
     * <p>This option specifies the initial delay duration for the exponential backoff retry strategy.
     * The delay will increase exponentially with each retry attempt, up to the maximum delay.
     *
     * <p>Default value: 300 milliseconds
     */
    public static final ConfigOption<Duration> RETRY_STRATEGY_MIN_DELAY_OPTION =
            ConfigOptions.key("retry-strategy.delay.min")
                    .durationType()
                    .defaultValue(Duration.ofMillis(300))
                    .withDescription("Base delay for the exponential backoff retry strategy");

    /**
     * Max delay for the exponential backoff retry strategy.
     *
     * <p>This option specifies the maximum delay duration for the exponential backoff retry strategy.
     * The delay will increase exponentially with each retry attempt, but will not exceed this maximum.
     *
     * <p>Default value: 1000 milliseconds
     */
    public static final ConfigOption<Duration> RETRY_STRATEGY_MAX_DELAY_OPTION =
            ConfigOptions.key("retry-strategy.delay.max")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription("Max delay for the exponential backoff retry strategy");

    /**
     * Maximum number of attempts for the exponential backoff retry strategy.
     *
     * <p>This option specifies the maximum number of retry attempts for the exponential backoff retry strategy.
     * After this many attempts, the operation will fail.
     *
     * <p>Default value: 50
     */
    public static final ConfigOption<Integer> RETRY_STRATEGY_MAX_ATTEMPTS_OPTION =
            ConfigOptions.key("retry-strategy.attempts.max")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Maximum number of attempts for the exponential backoff retry strategy");
}
