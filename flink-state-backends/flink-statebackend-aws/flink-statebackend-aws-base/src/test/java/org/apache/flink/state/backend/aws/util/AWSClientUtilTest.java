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

package org.apache.flink.state.backend.aws.util;

import org.apache.flink.state.backend.aws.config.AWSConfigConstants;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AWSClientUtil}.
 */
public class AWSClientUtilTest {

    @Test
    public void testFormatFlinkUserAgentPrefix() {
        // Arrange
        String userAgentFormat = "Flink-Test/%s-%s";

        // Act
        String result = AWSClientUtil.formatFlinkUserAgentPrefix(userAgentFormat);

        // Assert
        assertNotNull(result);
        assertTrue(result.startsWith("Flink-Test/"));
    }

    @Test
    public void testCreateClientOverrideConfiguration() {
        // Arrange
        SdkClientConfiguration config = SdkClientConfiguration.builder().build();
        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
        String userAgentPrefix = "Flink-Test";

        // Act
        ClientOverrideConfiguration overrideConfig =
            AWSClientUtil.createClientOverrideConfiguration(config, builder, userAgentPrefix);

        // Assert
        assertNotNull(overrideConfig);
    }

    @Test
    public void testGetCredentialsProviderBasic() {
        // Arrange
        Properties props = new Properties();
        props.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        props.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "test-access-key");
        props.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "test-secret-key");

        // Act
        AwsCredentialsProvider credentialsProvider = AWSGeneralUtil.getCredentialsProvider(props);

        // Assert
        assertNotNull(credentialsProvider);
        AwsCredentials credentials = credentialsProvider.resolveCredentials();
        assertTrue(credentials instanceof AwsBasicCredentials);
        assertEquals("test-access-key", credentials.accessKeyId());
        assertEquals("test-secret-key", credentials.secretAccessKey());
    }
}
