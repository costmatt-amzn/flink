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

package org.apache.flink.state.backend.aws.s3.config;

/**
 * Constants for the S3 state backend configuration.
 *
 * <p>This class defines constant values used throughout the S3 state backend implementation,
 * including configuration keys, user agent formats, and default values.
 *
 * <p>These constants are used to:
 * <ul>
 *   <li>Define configuration keys for S3-specific settings</li>
 *   <li>Set up user agent information for AWS API requests</li>
 *   <li>Provide consistent naming across the S3 state backend implementation</li>
 * </ul>
 */
public class S3StateBackendConstants {
    /**
     * Format string for the S3 client user agent prefix.
     *
     * <p>This format string is used to create a user agent prefix that identifies
     * requests coming from the Flink S3 state backend. The format includes
     * the Flink version and runtime information.
     *
     * <p>Format parameters:
     * <ol>
     *   <li>Flink version</li>
     *   <li>Runtime information (e.g., Java version)</li>
     * </ol>
     */
    public static final String BASE_S3_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) S3 State Backend";

    /**
     * Configuration key for the S3 client user agent prefix.
     *
     * <p>This key is used to set a custom user agent prefix for S3 clients.
     * The user agent helps identify and track API requests from the Flink state backend.
     */
    public static final String S3_CLIENT_USER_AGENT_PREFIX =
            "aws.s3.client.user-agent-prefix";


    /** The key for the S3 bucket name configuration. */
    public static final String BUCKET_NAME_KEY = "state.backend.s3.bucket";

    /** The key for the S3 base path configuration. */
    public static final String BASE_PATH_KEY = "state.backend.s3.base-path";

    /** The key for the S3 checkpoint prefix configuration. */
    public static final String CHECKPOINT_PREFIX_KEY = "state.backend.s3.checkpoint-prefix";

    /** The key for the S3 savepoint prefix configuration. */
    public static final String SAVEPOINT_PREFIX_KEY = "state.backend.s3.savepoint-prefix";

    /** The key for the S3 state file compression configuration. */
    public static final String STATE_FILE_COMPRESSION_KEY = "state.backend.s3.state-file-compression";

    /** The key for the S3 state file encryption configuration. */
    public static final String STATE_FILE_ENCRYPTION_KEY = "state.backend.s3.state-file-encryption";

    /** The key for the S3 state file part size configuration. */
    public static final String STATE_FILE_PART_SIZE_KEY = "state.backend.s3.state-file-part-size";

    private S3StateBackendConstants() {
        // Prevent instantiation
    }
}