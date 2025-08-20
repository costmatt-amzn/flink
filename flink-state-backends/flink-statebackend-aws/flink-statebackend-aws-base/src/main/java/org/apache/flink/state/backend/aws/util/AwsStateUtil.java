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

package org.apache.flink.state.backend.aws.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

import java.io.IOException;

/**
 * Utility class for AWS state backends.
 *
 * <p>This class provides common utility methods for state operations in AWS-based state backends,
 * including serialization, deserialization, and key extraction from serialized data.
 *
 * <p>The utilities in this class handle the complexities of Flink's composite key format,
 * which combines key group, key, and namespace information into a single serialized representation.
 * This format is used to efficiently store and retrieve state data in AWS services like
 * DynamoDB and S3.
 *
 * <p>Key features:
 * <ul>
 *   <li>Extraction of key group information from serialized data</li>
 *   <li>Extraction of keys and namespaces from serialized composite keys</li>
 *   <li>Handling of ambiguous key scenarios</li>
 * </ul>
 */
public class AwsStateUtil {
    /**
     * Reads the key group from serialized state data.
     *
     * <p>This method extracts the key group from the serialized composite key
     * stored in AWS services like DynamoDB or S3. The key group is used for
     * partitioning state across parallel instances of operators.
     *
     * @param keyGroupPrefixBytes The number of bytes in the key group prefix
     * @param valueBytes The serialized state data containing the key group
     * @return The key group, or Integer.MIN_VALUE if the data is null
     * @throws RuntimeException if an error occurs during deserialization
     */
    public static int readKeyGroup(
            final int keyGroupPrefixBytes,
            final byte[] valueBytes) {
        if (valueBytes == null) {
            return Integer.MIN_VALUE;
        }

        try {
            return CompositeKeySerializationUtils.readKeyGroup(
                    keyGroupPrefixBytes,
                    new DataInputDeserializer(valueBytes)
            );
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the key from serialized state data.
     *
     * <p>This method extracts the key from the serialized composite key
     * stored in AWS services like DynamoDB or S3. The key is the primary
     * identifier for state entries within a key group.
     *
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param valueBytes The serialized state data containing the key
     * @param <K> The type of the key
     * @param <N> The type of the namespace
     * @return The key, or null if the data is null
     * @throws RuntimeException if an error occurs during deserialization
     */
    public static <K, N> K readKey(
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final byte[] valueBytes) {
        if (valueBytes == null) {
            return null;
        }

        try {
            return CompositeKeySerializationUtils.readKey(
                    keySerializer,
                    new DataInputDeserializer(valueBytes),
                    CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            keySerializer,
                            namespaceSerializer));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the namespace from serialized state data.
     *
     * <p>This method extracts the namespace from the serialized composite key
     * stored in AWS services like DynamoDB or S3. The namespace is used to
     * organize state entries within the same key into logical groups.
     *
     * @param keySerializer The serializer for the key
     * @param namespaceSerializer The serializer for the namespace
     * @param valueBytes The serialized state data containing the namespace
     * @param <K> The type of the key
     * @param <N> The type of the namespace
     * @return The namespace, or null if the data is null
     * @throws RuntimeException if an error occurs during deserialization
     */
    public static <K, N> N readNamespace(
            final TypeSerializer<K> keySerializer,
            final TypeSerializer<N> namespaceSerializer,
            final byte[] valueBytes) {
        if (valueBytes == null) {
            return null;
        }

        try {
            return CompositeKeySerializationUtils.readNamespace(
                    namespaceSerializer,
                    new DataInputDeserializer(valueBytes),
                    CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                            keySerializer,
                            namespaceSerializer));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AwsStateUtil() {
        // Prevent instantiation
    }
}
