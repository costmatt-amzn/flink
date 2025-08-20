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

package org.apache.flink.state.backend.aws.s3.util;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;

/**
 * Utility class for building S3 keys for state entries.
 *
 * <p>This class helps construct the object keys used in the S3 state backend.
 * It creates S3 object keys that uniquely identify state entries based on the
 * job, task, state name, key, and namespace.
 *
 * <p>The key structure follows the pattern:
 * <pre>{@code [basePath/]jobName/taskName/stateName/encodedKey/encodedNamespace/state}</pre>
 * where:
 * <ul>
 *   <li>{@code basePath} is the optional base path configured for the state backend</li>
 *   <li>{@code jobName} is the name of the Flink job</li>
 *   <li>{@code taskName} is the name of the task</li>
 *   <li>{@code stateName} is the name of the state (from the state descriptor)</li>
 *   <li>{@code encodedKey} is the Base64-encoded serialized key</li>
 *   <li>{@code encodedNamespace} is the Base64-encoded serialized namespace</li>
 * </ul>
 *
 * <p>This key structure provides several benefits:
 * <ul>
 *   <li>Hierarchical organization for efficient browsing and management</li>
 *   <li>Clear separation between different jobs, tasks, and states</li>
 *   <li>Support for listing all state entries for a specific job, task, or state name</li>
 *   <li>Unique identification of each state entry</li>
 * </ul>
 *
 * <p>The class also provides methods to decode the serialized key and namespace
 * from an S3 key, which is useful for state iteration and restoration.
 */
public final class S3KeyAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(S3KeyAdapter.class);

    /**
     * Builds an S3 key for the given state name and serialized key/namespace.
     *
     * <p>This method creates an S3 object key that uniquely identifies a state entry
     * based on the job, task, state name, and the serialized key and namespace. The key follows
     * the pattern: {@code [basePath/]jobName/taskName/stateName/encodedKey/encodedNamespace/state}
     *
     * <p>The serialized key and namespace are Base64-encoded to ensure they can be safely
     * used as part of the S3 object key.
     *
     * @param jobInfo The job information containing job ID and name
     * @param taskInfo The task information containing task name and index
     * @param serializedKey The serialized key as a byte array
     * @param serializedNamespace The serialized namespace as a byte array (can be null)
     * @param stateName The name of the state (from the state descriptor)
     * @param configuration The configuration containing S3 path settings
     * @return The S3 object key string
     * @throws IOException If an error occurs while building the key
     */
    public static String createS3Key(
            final JobInfo jobInfo,
            final TaskInfo taskInfo,
            final byte[] serializedKey,
            final byte[] serializedNamespace,
            final String stateName,
            final Configuration configuration) throws IOException {
        LOG.debug(
                "Creating S3 key for job: {}, task: {}, state: {}",
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName);

        final String basePath = configuration.get(S3StateBackendOptions.BASE_PATH);

        LOG.trace("Using base path: '{}'", basePath);

        // Build the S3 key
        final StringBuilder sb = new StringBuilder();

        if (!basePath.isEmpty()) {
            sb.append(basePath);
            if (!basePath.endsWith("/")) {
                sb.append("/");
                LOG.trace("Added trailing slash to base path");
            }
        }

        final String keyPath = String.join(
                "/",
                jobInfo.getJobName(),
                taskInfo.getTaskName(),
                stateName,
                Base64.getEncoder().encodeToString(serializedKey));

        LOG.trace("Key path components joined: {}", keyPath);

        sb.append(keyPath);

        if (serializedNamespace != null) {
            sb.append("/");
            sb.append(Base64.getEncoder().encodeToString(serializedNamespace));
        }

        // Always append "state" as the filename
        sb.append("/state");

        final String s3Key = sb.toString();
        LOG.debug("S3 key created: {}", s3Key);
        return s3Key;
    }

    /**
     * Extracts the serialized key from an S3 key.
     *
     * <p>This method decodes the Base64-encoded serialized key from an S3 key.
     * It parses the S3 key path to find the key component and decodes it.
     *
     * @param s3Key The S3 key to extract the serialized key from
     * @return The decoded serialized key as a byte array
     */

    public static byte[] decodeSerializedKey(final String s3Key) {
        LOG.trace("Extracting key and namespace from S3 key: {}", s3Key);
        // jobName / taskName / stateDescriptor / serializedKeyAndNamespace / state
        final String[] pathComponents = s3Key.split("/");
        return Base64.getDecoder().decode(pathComponents[pathComponents.length - 3]);
    }

    public static byte[] decodeSerializedNamespace(final String s3Key) {
        LOG.trace("Extracting key and namespace from S3 key: {}", s3Key);
        // jobName / taskName / stateDescriptor / serializedKeyAndNamespace / state

        final String[] pathComponents = s3Key.split("/");
        return Base64.getDecoder().decode(pathComponents[pathComponents.length - 2]);
    }
}
