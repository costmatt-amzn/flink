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

package org.apache.flink.state.backend.aws.dynamodb.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.backend.aws.metrics.AwsStateBackendMetrics;

/**
 * Metrics for the DynamoDB state backend.
 *
 * <p>This class provides metrics for monitoring the performance and behavior of the DynamoDB state backend.
 * It extends the base AWS state backend metrics with DynamoDB-specific tracking for state operations.
 */
public class DynamoDbStateBackendMetrics extends AwsStateBackendMetrics {

    /**
     * Creates a new DynamoDbStateBackendMetrics instance.
     *
     * @param metricGroup The metric group to register metrics with
     */
    public DynamoDbStateBackendMetrics(final MetricGroup metricGroup) {
        super(metricGroup, "dynamodb");
    }
}