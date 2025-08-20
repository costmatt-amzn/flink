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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/**
 * Configuration constants for AWS service usage.
 *
 * <p>This class provides string constants for configuration keys used to configure
 * AWS services in Flink applications. These constants are used in conjunction with
 * Flink's configuration system to set up AWS credentials, regions, endpoints, and
 * other AWS-specific settings.
 *
 * <p>The constants defined here are used by the AWS state backends and other
 * AWS-integrated components in Flink.
 *
 * <p>Example usage:
 * <pre>{@code
 * Configuration config = new Configuration();
 * config.setString(AWSConfigConstants.AWS_REGION, "us-west-2");
 * config.setString(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, AWSConfigConstants.CredentialProvider.BASIC.name());
 * config.setString(AWSConfigConstants.AWS_ACCESS_KEY_ID, "your-access-key");
 * config.setString(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "your-secret-key");
 * }</pre>
 */
@PublicEvolving
public class AWSConfigConstants {

    /**
     * Possible configuration values for the type of credential provider to use when accessing AWS.
     *
     * <p>Internally, a corresponding implementation of {@link AwsCredentialsProvider} will be used
     * based on the selected provider type.
     *
     * <p>Each credential provider type has its own requirements and behavior:
     * <ul>
     *   <li>{@link #ENV_VAR}: Uses environment variables for credentials</li>
     *   <li>{@link #SYS_PROP}: Uses Java system properties for credentials</li>
     *   <li>{@link #PROFILE}: Uses AWS credentials profile file</li>
     *   <li>{@link #BASIC}: Uses explicitly provided access key and secret key</li>
     *   <li>{@link #ASSUME_ROLE}: Assumes an IAM role using provided credentials</li>
     *   <li>{@link #WEB_IDENTITY_TOKEN}: Uses a web identity token to assume a role</li>
     *   <li>{@link #CUSTOM}: Uses a custom credentials provider class</li>
     *   <li>{@link #AUTO}: Uses a credentials provider chain that tries multiple methods</li>
     * </ul>
     */
    public enum CredentialProvider {

        /**
         * Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create
         * AWS credentials.
         */
        ENV_VAR,

        /**
         * Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS
         * credentials.
         */
        SYS_PROP,

        /**
     Use a AWS credentials profile file to create the AWS credentials. */
        PROFILE,

        /**
         * Simply create AWS credentials by supplying the AWS access key ID and AWS secret key in
         * the configuration properties.
         */
        BASIC,

        /**
         * Create AWS credentials by assuming a role. The credentials for assuming the role must be
         * supplied. *
         */
        ASSUME_ROLE,

        /**
         * Use AWS WebIdentityToken in order to assume a role. A token file and role details can be
         * supplied as configuration or environment variables. *
         */
        WEB_IDENTITY_TOKEN,

        /**
     Use a custom class specified by the user in connector config. */
        CUSTOM,

        /**
         * A credentials provider chain will be used that searches for credentials in this order:
         * ENV_VARS, SYS_PROPS, WEB_IDENTITY_TOKEN, PROFILE in the AWS instance metadata. *
         */
        AUTO,
    }

    /**
     * The AWS region of the service ("us-east-1" is used if not set).
     *
     * <p>This setting specifies the AWS region where the service is located.
     * If not set, the default region "us-east-1" will be used.
     *
     * <p>Example: "us-west-2"
     */
    public static final String AWS_REGION = "aws.region";

    /**
     * The credential provider type to use when AWS credentials are required (BASIC is used if not
     * set).
     *
     * <p>This setting specifies which credential provider type to use for AWS authentication.
     * The value should be one of the {@link CredentialProvider} enum values.
     *
     * <p>Example: "BASIC"
     */
    public static final String AWS_CREDENTIALS_PROVIDER = "aws.credentials.provider";

    /**
     * The AWS access key ID to use when setting credentials provider type to BASIC.
     *
     * <p>This setting is required when using the BASIC credential provider type.
     *
     * <p>Example: "AKIAIOSFODNN7EXAMPLE"
     */
    public static final String AWS_ACCESS_KEY_ID = accessKeyId(AWS_CREDENTIALS_PROVIDER);

    /**
     * The AWS secret key to use when setting credentials provider type to BASIC.
     *
     * <p>This setting is required when using the BASIC credential provider type.
     *
     * <p>Example: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
     */
    public static final String AWS_SECRET_ACCESS_KEY = secretKey(AWS_CREDENTIALS_PROVIDER);

    /**
     * Optional configuration for profile path if credential provider type is set to be PROFILE.
     *
     * <p>This setting specifies the path to the AWS credentials profile file.
     * If not set, the default profile path will be used.
     *
     * <p>Example: "/path/to/credentials"
     */
    public static final String AWS_PROFILE_PATH = profilePath(AWS_CREDENTIALS_PROVIDER);

    /**
     * Optional configuration for profile name if credential provider type is set to be PROFILE.
     *
     * <p>This setting specifies the name of the profile to use from the AWS credentials profile file.
     * If not set, the default profile will be used.
     *
     * <p>Example: "production"
     */
    public static final String AWS_PROFILE_NAME = profileName(AWS_CREDENTIALS_PROVIDER);

    /**
     * The AWS endpoint for the STS (derived from the AWS region setting if not set) to use if
     * credential provider type is set to be ASSUME_ROLE.
     *
     * <p>This setting specifies the endpoint URL for the AWS Security Token Service (STS).
     * If not set, the endpoint will be derived from the AWS region setting.
     *
     * <p>Example: "https://sts.us-west-2.amazonaws.com"
     */
    public static final String AWS_ROLE_STS_ENDPOINT = roleStsEndpoint(AWS_CREDENTIALS_PROVIDER);

    /**
     * The full path (e.g. org.user_company.auth.CustomAwsCredentialsProvider) to the user provided
     * class to use if credential provider type is set to be CUSTOM.
     *
     * <p>This setting specifies the fully qualified class name of a custom AWS credentials provider.
     * The class must implement {@link AwsCredentialsProvider} and have a no-argument constructor.
     *
     * <p>Example: "com.example.CustomAwsCredentialsProvider"
     */
    public static final String CUSTOM_CREDENTIALS_PROVIDER_CLASS =
            customCredentialsProviderClass(AWS_CREDENTIALS_PROVIDER);

    /**
     * The role ARN to use when credential provider type is set to ASSUME_ROLE or
     * WEB_IDENTITY_TOKEN.
     *
     * <p>This setting specifies the Amazon Resource Name (ARN) of the role to assume.
     *
     * <p>Example: "arn:aws:iam::123456789012:role/flink-role"
     */
    public static final String AWS_ROLE_ARN = roleArn(AWS_CREDENTIALS_PROVIDER);

    /**
     * The role session name to use when credential provider type is set to ASSUME_ROLE or
     * WEB_IDENTITY_TOKEN.
     *
     * <p>This setting specifies the name of the session when assuming a role.
     *
     * <p>Example: "flink-session"
     */
    public static final String AWS_ROLE_SESSION_NAME = roleSessionName(AWS_CREDENTIALS_PROVIDER);

    /**
     * The external ID to use when credential provider type is set to ASSUME_ROLE.
     *
     * <p>This setting specifies the external ID to use when assuming a role.
     * The external ID is a unique identifier that might be required when you assume a role in another account.
     *
     * <p>Example: "12345"
     */
    public static final String AWS_ROLE_EXTERNAL_ID = externalId(AWS_CREDENTIALS_PROVIDER);

    /**
     * The absolute path to the web identity token file that should be used if provider type is se
     * to WEB_IDENTITY_TOKEN.
     *
     * <p>This setting specifies the path to the web identity token file.
     *
     * <p>Example: "/path/to/token/file"
     */
    public static final String AWS_WEB_IDENTITY_TOKEN_FILE =
            webIdentityTokenFile(AWS_CREDENTIALS_PROVIDER);

    /**
     * The credentials provider that provides credentials for assuming the role when credential
     * provider type is set to ASSUME_ROLE. Roles can be nested, so AWS_ROLE_CREDENTIALS_PROVIDER
     * can again be set to "ASSUME_ROLE"
     *
     * <p>This setting specifies the credential provider to use for assuming a role.
     * The value should be one of the {@link CredentialProvider} enum values.
     *
     * <p>Example: "BASIC"
     */
    public static final String AWS_ROLE_CREDENTIALS_PROVIDER =
            roleCredentialsProvider(AWS_CREDENTIALS_PROVIDER);

    /**
     * The AWS endpoint for the service (derived from the AWS region setting if not set).
     *
     * <p>This setting specifies the endpoint URL for the AWS service.
     * If not set, the endpoint will be derived from the AWS region setting.
     *
     * <p>Example: "https://dynamodb.us-west-2.amazonaws.com"
     */
    public static final String AWS_ENDPOINT = "aws.endpoint";

    /**
     * Whether to trust all SSL certificates.
     *
     * <p>This setting specifies whether to trust all SSL certificates, including self-signed certificates.
     * This should only be set to true in development or testing environments.
     *
     * <p>Example: "false"
     */
    public static final String TRUST_ALL_CERTIFICATES = "aws.trust.all.certificates";

    /**
     * The HTTP protocol version to use.
     *
     * <p>This setting specifies the HTTP protocol version to use for AWS service requests.
     *
     * <p>Example: "HTTP1_1" or "HTTP2"
     */
    public static final String HTTP_PROTOCOL_VERSION = "aws.http.protocol.version";

    /**
     * Maximum request concurrency for {@link SdkAsyncHttpClient}.
     *
     * <p>This setting specifies the maximum number of concurrent requests for the AWS SDK async HTTP client.
     *
     * <p>Example: "50"
     */
    public static final String HTTP_CLIENT_MAX_CONCURRENCY = "aws.http-client.max-concurrency";

    /**
     * Read Request timeout for {@link SdkAsyncHttpClient}.
     *
     * <p>This setting specifies the read timeout in milliseconds for the AWS SDK async HTTP client.
     *
     * <p>Example: "5000"
     */
    public static final String HTTP_CLIENT_READ_TIMEOUT_MILLIS = "aws.http-client.read-timeout";

    /**
     * Generates the configuration key for the access key ID for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the access key ID
     */
    public static String accessKeyId(final String prefix) {
        return prefix + ".basic.accesskeyid";
    }

    /**
     * Generates the configuration key for the secret key for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the secret key
     */
    public static String secretKey(final String prefix) {
        return prefix + ".basic.secretkey";
    }

    /**
     * Generates the configuration key for the profile path for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the profile path
     */
    public static String profilePath(final String prefix) {
        return prefix + ".profile.path";
    }

    /**
     * Generates the configuration key for the profile name for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the profile name
     */
    public static String profileName(final String prefix) {
        return prefix + ".profile.name";
    }

    /**
     * Generates the configuration key for the role ARN for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the role ARN
     */
    public static String roleArn(final String prefix) {
        return prefix + ".role.arn";
    }

    /**
     * Generates the configuration key for the role session name for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the role session name
     */
    public static String roleSessionName(final String prefix) {
        return prefix + ".role.sessionName";
    }

    /**
     * Generates the configuration key for the external ID for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the external ID
     */
    public static String externalId(final String prefix) {
        return prefix + ".role.externalId";
    }

    /**
     * Generates the configuration key for the role credentials provider for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the role credentials provider
     */
    public static String roleCredentialsProvider(final String prefix) {
        return prefix + ".role.provider";
    }

    /**
     * Generates the configuration key for the role STS endpoint for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the role STS endpoint
     */
    public static String roleStsEndpoint(final String prefix) {
        return prefix + ".role.stsEndpoint";
    }

    /**
     * Generates the configuration key for the custom credentials provider class for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the custom credentials provider class
     */
    public static String customCredentialsProviderClass(final String prefix) {
        return prefix + ".custom.class";
    }

    /**
     * Generates the configuration key for the web identity token file for a specific credentials provider prefix.
     *
     * @param prefix The credentials provider prefix
     * @return The configuration key for the web identity token file
     */
    public static String webIdentityTokenFile(final String prefix) {
        return prefix + ".webIdentityToken.file";
    }
}
