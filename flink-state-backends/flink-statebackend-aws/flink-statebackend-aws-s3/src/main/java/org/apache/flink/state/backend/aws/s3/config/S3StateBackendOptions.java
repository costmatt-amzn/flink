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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options specific to the S3 state backend.
 *
 * <p>These options control the behavior of the S3 state backend, including
 * bucket name, path settings, and file options.
 *
 * <p>These options can be set in the Flink configuration or directly when
 * creating the S3 state backend programmatically.
 *
 * <p>Example usage:
 * <pre>{@code
 * Configuration config = new Configuration();
 * config.set(S3StateBackendOptions.BUCKET_NAME, "flink-state");
 * config.set(S3StateBackendOptions.BASE_PATH, "my-job/state");
 * config.set(S3StateBackendOptions.STATE_FILE_COMPRESSION, true);
 * }</pre>
 */
public class S3StateBackendOptions {

    /**
     * The S3 bucket name.
     *
     * <p>This option specifies the name of the S3 bucket to use for state storage.
     * The bucket must exist and be accessible with the configured AWS credentials.
     *
     * <p>This option is required and has no default value.
     */
    public static final ConfigOption<String> BUCKET_NAME = ConfigOptions
            .key("state.backend.s3.bucket")
            .stringType()
            .noDefaultValue()
            .withDescription("The S3 bucket name.");

    /**
     * The base path within the S3 bucket.
     *
     * <p>This option specifies the base path within the S3 bucket where state files
     * will be stored. This allows multiple jobs to share the same bucket by using
     * different base paths.
     *
     * <p>Default value: empty string (root of the bucket)
     */
    public static final ConfigOption<String> BASE_PATH = ConfigOptions
            .key("state.backend.s3.base-path")
            .stringType()
            .defaultValue("")
            .withDescription("The base path within the S3 bucket.");



    /**
     * The prefix for checkpoint files.
     *
     * <p>This option specifies the prefix to use for checkpoint files within the base path.
     *
     * <p>Default value: "checkpoints"
     */
    public static final ConfigOption<String> CHECKPOINT_PREFIX = ConfigOptions
            .key("state.backend.s3.checkpoint-prefix")
            .stringType()
            .defaultValue("checkpoints")
            .withDescription("The prefix for checkpoint files.");

    /**
     * The prefix for savepoint files.
     *
     * <p>This option specifies the prefix to use for savepoint files within the base path.
     *
     * <p>Default value: "savepoints"
     */
    public static final ConfigOption<String> SAVEPOINT_PREFIX = ConfigOptions
            .key("state.backend.s3.savepoint-prefix")
            .stringType()
            .defaultValue("savepoints")
            .withDescription("The prefix for savepoint files.");



    /**
     * Whether to use compression for state files.
     *
     * <p>When set to true, state files will be compressed before being stored in S3.
     * This reduces storage costs but increases CPU usage.
     *
     * <p>Default value: true
     */
    public static final ConfigOption<Boolean> STATE_FILE_COMPRESSION = ConfigOptions
            .key("state.backend.s3.state-file-compression")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to use compression for state files.");

    /**
     * Whether to use encryption for state files.
     *
     * <p>When set to true, state files will be encrypted before being stored in S3.
     * This increases security but may impact performance.
     *
     * <p>Default value: false
     */
    public static final ConfigOption<Boolean> STATE_FILE_ENCRYPTION = ConfigOptions
            .key("state.backend.s3.state-file-encryption")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use encryption for state files.");

    /**
     * The part size for multipart uploads of state files.
     *
     * <p>This option specifies the part size in bytes to use for multipart uploads
     * of state files to S3. Larger part sizes can improve throughput but require
     * more memory.
     *
     * <p>Default value: 5 MB
     */
    public static final ConfigOption<Integer> STATE_FILE_PART_SIZE = ConfigOptions
            .key("state.backend.s3.state-file-part-size")
            .intType()
            .defaultValue(5 * 1024 * 1024)
            .withDescription("The part size for multipart uploads of state files.");

    private S3StateBackendOptions() {
        // Prevent instantiation
    }
}
