/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License att
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.dynamodb.config;

/**
 * A container for DynamoDB attribute names used in the state backend.
 *
 * <p>This class defines the attribute names used in the DynamoDB table schema
 * for storing Flink state. These attributes form the structure of the DynamoDB
 * table and are used for querying and storing state data.
 *
 * <p>The DynamoDB table uses a composite primary key consisting of:
 * <ul>
 *   <li>Partition key: {@link #JOB_AWARE_STATE_DESC_ATTR} - Contains job name, task name, and state name</li>
 *   <li>Sort key: {@link #KEY_AND_NAMESPACE_ATTR} - Contains key and namespace information</li>
 * </ul>
 *
 * <p>Additional attributes store the actual state values and metadata, including:
 * <ul>
 *   <li>State value: {@link #VALUE_ATTR} - The serialized state value</li>
 *   <li>TTL: {@link #TTL_ATTR} - Time-to-live for state expiration</li>
 *   <li>Metadata: Various attributes for job, task, and state information</li>
 * </ul>
 *
 * <p>This design provides several benefits:
 * <ul>
 *   <li>Efficient partitioning of state data across DynamoDB</li>
 *   <li>Fast lookups by key and namespace</li>
 *   <li>Support for TTL-based state expiration</li>
 *   <li>Rich metadata for debugging and monitoring</li>
 * </ul>
 */
public final class DynamoDbAttributes {

    /**
     * Partition key attribute name for DynamoDB tables.
     *
     * <p>This attribute serves as the partition key in the DynamoDB table's primary key.
     * It contains a combination of job name, task name, state descriptor, and state key to efficiently
     * partition state data across the DynamoDB table.
     *
     * <p>Format: "{jobName} -> {taskName} -> {stateName} -> {stateKey}"
     *
     * <p>Using this format ensures good distribution of data across DynamoDB partitions
     * while keeping related state entries together.
     */
    public static final String JOB_AWARE_STATE_KEY_AND_DESCRIPTOR_ATTR = "jobAwareStateKeyAndDescriptor";

    /**
     * Name of the Global Secondary Index for querying by job name and task name.
     */
    public static final String GSI_JOB_NAME_TASK_NAME_INDEX = "JobNameTaskNameIndex";

    /**
     * Job name and task name composite attribute for GSI partition key.
     *
     * <p>Format: "{jobName} -> {taskName}"
     */
    public static final String JOB_NAME_AND_TASK_NAME_ATTR = "jobNameAndTaskName";

    /**
     * Sort key attribute name for DynamoDB tables.
     * The namespace now serves directly as the sort key.
     */
    public static final String NAMESPACE_ATTR = "namespace";

    // Removed deprecated attributes as they're not needed for the new schema

    /**
     * Value attribute name for DynamoDB tables.
     *
     * <p>This attribute stores the serialized state value data as a binary (B) attribute.
     * The value is compressed using Snappy compression to reduce storage requirements
     * and network transfer costs.
     *
     * <p>For different state types, the serialization format differs:
     * <ul>
     *   <li>ValueState: The serialized value</li>
     *   <li>ListState: The serialized list of values</li>
     *   <li>MapState: The serialized map of key-value pairs</li>
     *   <li>ReducingState: The serialized reduced value</li>
     *   <li>AggregatingState: The serialized accumulator</li>
     * </ul>
     */
    public static final String VALUE_ATTR = "value";

    /**
     * TTL attribute name for DynamoDB tables.
     *
     * <p>This attribute stores the expiration time for state entries when TTL is enabled.
     * DynamoDB automatically removes items after their TTL timestamp has passed.
     * The value is stored as epoch seconds (N attribute type).
     *
     * <p>When a state descriptor has TTL enabled, this attribute is set based on:
     * <ul>
     *   <li>Current time + TTL duration for new state entries</li>
     *   <li>Updated TTL value when state is accessed (for OnReadAndWrite update type)</li>
     * </ul>
     *
     * <p>DynamoDB's TTL feature typically removes expired items within 48 hours of expiration.
     */
    public static final String TTL_ATTR = "ttl";

    /**
     * State descriptor attribute name for DynamoDB tables.
     *
     * <p>This attribute stores the name of the state descriptor, which identifies
     * the specific state variable within an operator (e.g., "windowState", "valueState").
     * It's used as part of the partition key to group related state entries.
     */
    public static final String STATE_DESCRIPTOR_ATTR = "state_descriptor";

    /**
     * Job ID attribute for DynamoDB tables.
     */
    public static final String JOB_ID_ATTR = "job_id";

    /**
     * Job Name attribute for DynamoDB tables.
     *
     * <p>This attribute stores the unique identifier of the Flink operator that owns the state.
     * It's used as part of the partition key to group all state entries for a specific operator.
     */
    public static final String JOB_NAME_ATTR = "job_name";

    /**
     * Operator ID attribute for DynamoDB tables.
     */
    public static final String OPERATOR_ID_ATTR = "operator_id";

    /**
     * Task name attribute for DynamoDB tables.
     *
     * <p>This attribute stores the name of the task that created the state entry.
     */
    public static final String TASK_NAME_ATTR = "task_name";

    /**
     * Task index attribute for DynamoDB tables.
     *
     * <p>This attribute stores the index of the parallel subtask that created the state entry.
     */
    public static final String TASK_INDEX_ATTR = "task_index";

    // Namespace attribute is already defined above

    /**
     * State key attribute for DynamoDB tables.
     *
     * <p>This attribute stores the serialized key of the state entry.
     * For keyed state, this contains the user key that identifies the specific state entry.
     * It's used as part of the sort key to enable efficient lookups.
     */
    public static final String STATE_KEY_ATTR = "state_key";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private DynamoDbAttributes() {
    }
}
