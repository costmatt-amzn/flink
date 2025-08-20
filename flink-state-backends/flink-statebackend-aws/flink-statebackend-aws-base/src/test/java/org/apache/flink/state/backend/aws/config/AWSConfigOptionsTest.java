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

package org.apache.flink.state.backend.aws.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link AWSConfigOptions}.
 */
public class AWSConfigOptionsTest {

    @Test
    public void testDefaultValues() {
        ReadableConfig config = new Configuration();

        // Test default values
        assertEquals(Duration.ofMillis(300),
                config.get(AWSConfigOptions.RETRY_STRATEGY_MIN_DELAY_OPTION));
        assertEquals(Duration.ofMillis(1000),
                config.get(AWSConfigOptions.RETRY_STRATEGY_MAX_DELAY_OPTION));
        assertEquals(50,
                config.get(AWSConfigOptions.RETRY_STRATEGY_MAX_ATTEMPTS_OPTION));
    }

    @Test
    public void testCustomValues() {
        Configuration config = new Configuration();

        // Set custom values
        config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
        config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, AWSConfigConstants.CredentialProvider.BASIC);
        config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "test-access-key");
        config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "test-secret-key");
        config.set(AWSConfigOptions.AWS_ENDPOINT_OPTION, "http://localhost:4566");

        // Verify custom values
        assertEquals("us-west-2", config.get(AWSConfigOptions.AWS_REGION_OPTION));
        assertEquals(AWSConfigConstants.CredentialProvider.BASIC,
                config.get(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION));
        assertEquals("test-access-key", config.get(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION));
        assertEquals("test-secret-key", config.get(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION));
        assertEquals("http://localhost:4566", config.get(AWSConfigOptions.AWS_ENDPOINT_OPTION));
    }

    @Test
    public void testRetryStrategyConfiguration() {
        Configuration config = new Configuration();

        // Set custom retry strategy values
        config.set(AWSConfigOptions.RETRY_STRATEGY_MIN_DELAY_OPTION, Duration.ofMillis(500));
        config.set(AWSConfigOptions.RETRY_STRATEGY_MAX_DELAY_OPTION, Duration.ofMillis(2000));
        config.set(AWSConfigOptions.RETRY_STRATEGY_MAX_ATTEMPTS_OPTION, 100);

        // Verify custom retry strategy values
        assertEquals(Duration.ofMillis(500), config.get(AWSConfigOptions.RETRY_STRATEGY_MIN_DELAY_OPTION));
        assertEquals(Duration.ofMillis(2000), config.get(AWSConfigOptions.RETRY_STRATEGY_MAX_DELAY_OPTION));
        assertEquals(100, config.get(AWSConfigOptions.RETRY_STRATEGY_MAX_ATTEMPTS_OPTION));
    }
}
