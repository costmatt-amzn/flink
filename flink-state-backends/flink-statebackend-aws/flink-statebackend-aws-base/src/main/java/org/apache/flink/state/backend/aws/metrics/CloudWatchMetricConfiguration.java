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

package org.apache.flink.state.backend.aws.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.state.backend.aws.config.CloudWatchOptions;

/**
 * Configuration for the CloudWatch metric reporter.
 *
 * <p>This class encapsulates all configuration parameters needed by
 * the CloudWatchMetricReporter. It uses the builder pattern for construction
 * and provides methods to update from a MetricConfig.</p>
 */
@Internal
public class CloudWatchMetricConfiguration {

    private final int batchSize;
    private final boolean includeTaskDimensions;
    private final boolean parseMetricName;
    private final boolean extractTaskManagerDimension;
    private final boolean extractOperatorDimension;
    private final String namespace;
    private final String jobName;
    private final String taskName;

    /**
     * Private constructor, use the builder instead.
     */
    private CloudWatchMetricConfiguration(
            final int batchSize,
            final boolean includeTaskDimensions,
            final boolean parseMetricName,
            final boolean extractTaskManagerDimension,
            final boolean extractOperatorDimension,
            final String namespace,
            final String jobName,
            final String taskName) {
        this.batchSize = batchSize;
        this.includeTaskDimensions = includeTaskDimensions;
        this.parseMetricName = parseMetricName;
        this.extractTaskManagerDimension = extractTaskManagerDimension;
        this.extractOperatorDimension = extractOperatorDimension;
        this.namespace = namespace;
        this.jobName = jobName;
        this.taskName = taskName;
    }

    /**
     * Get the maximum batch size for CloudWatch API requests.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Check if task dimensions should be included.
     */
    public boolean isIncludeTaskDimensions() {
        return includeTaskDimensions;
    }

    /**
     * Check if metric names should be parsed for additional dimensions.
     */
    public boolean isParseMetricName() {
        return parseMetricName;
    }

    /**
     * Check if TaskManager IDs should be extracted as dimensions.
     */
    public boolean isExtractTaskManagerDimension() {
        return extractTaskManagerDimension;
    }

    /**
     * Check if operator names should be extracted as dimensions.
     */
    public boolean isExtractOperatorDimension() {
        return extractOperatorDimension;
    }

    /**
     * Get the CloudWatch namespace.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Get the job name.
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Get the task name.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * Create a new configuration with updated values from a MetricConfig.
     *
     * @param config The MetricConfig to get values from
     * @return A new CloudWatchMetricConfiguration with updated values
     */
    public CloudWatchMetricConfiguration withUpdatesFrom(final MetricConfig config) {
        return new Builder()
                .setBatchSize(config.getInteger(CloudWatchOptions.BATCH_SIZE.key(), this.batchSize))
                .setIncludeTaskDimensions(config.getBoolean(
                        CloudWatchOptions.INCLUDE_TASK_DIMENSIONS.key(), this.includeTaskDimensions))
                .setParseMetricName(config.getBoolean(
                        CloudWatchOptions.PARSE_METRIC_NAME.key(), this.parseMetricName))
                .setExtractTaskManagerDimension(config.getBoolean(
                        CloudWatchOptions.EXTRACT_TASKMANAGER_DIMENSION.key(), this.extractTaskManagerDimension))
                .setExtractOperatorDimension(config.getBoolean(
                        CloudWatchOptions.EXTRACT_OPERATOR_DIMENSION.key(), this.extractOperatorDimension))
                .setNamespace(config.getString(CloudWatchOptions.NAMESPACE.key(), this.namespace))
                .setJobName(config.getString(
                        CloudWatchOptions.JOB_NAME.key(),
                        config.getString("metrics.scope.job.name", this.jobName)))
                .setTaskName(config.getString(
                        CloudWatchOptions.TASK_NAME.key(),
                        config.getString("metrics.scope.task.name", this.taskName)))
                .build();
    }

    /**
     * Create a default configuration.
     *
     * @return A configuration with default values
     */
    public static CloudWatchMetricConfiguration fromDefaults() {
        return new Builder()
                .setBatchSize(CloudWatchOptions.BATCH_SIZE.defaultValue())
                .setIncludeTaskDimensions(CloudWatchOptions.INCLUDE_TASK_DIMENSIONS.defaultValue())
                .setParseMetricName(CloudWatchOptions.PARSE_METRIC_NAME.defaultValue())
                .setExtractTaskManagerDimension(CloudWatchOptions.EXTRACT_TASKMANAGER_DIMENSION.defaultValue())
                .setExtractOperatorDimension(CloudWatchOptions.EXTRACT_OPERATOR_DIMENSION.defaultValue())
                .setNamespace(CloudWatchOptions.NAMESPACE.defaultValue())
                .setJobName(CloudWatchOptions.JOB_NAME.defaultValue())
                .setTaskName(CloudWatchOptions.TASK_NAME.defaultValue())
                .build();
    }

    /**
     * Builder for CloudWatchMetricConfiguration.
     */
    public static class Builder {
        private int batchSize = CloudWatchOptions.BATCH_SIZE.defaultValue();
        private boolean includeTaskDimensions = CloudWatchOptions.INCLUDE_TASK_DIMENSIONS.defaultValue();
        private boolean parseMetricName = CloudWatchOptions.PARSE_METRIC_NAME.defaultValue();
        private boolean extractTaskManagerDimension = CloudWatchOptions.EXTRACT_TASKMANAGER_DIMENSION.defaultValue();
        private boolean extractOperatorDimension = CloudWatchOptions.EXTRACT_OPERATOR_DIMENSION.defaultValue();
        private String namespace = CloudWatchOptions.NAMESPACE.defaultValue();
        private String jobName = CloudWatchOptions.JOB_NAME.defaultValue();
        private String taskName = CloudWatchOptions.TASK_NAME.defaultValue();

        public Builder setBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setIncludeTaskDimensions(final boolean includeTaskDimensions) {
            this.includeTaskDimensions = includeTaskDimensions;
            return this;
        }

        public Builder setParseMetricName(final boolean parseMetricName) {
            this.parseMetricName = parseMetricName;
            return this;
        }

        public Builder setExtractTaskManagerDimension(final boolean extractTaskManagerDimension) {
            this.extractTaskManagerDimension = extractTaskManagerDimension;
            return this;
        }

        public Builder setExtractOperatorDimension(final boolean extractOperatorDimension) {
            this.extractOperatorDimension = extractOperatorDimension;
            return this;
        }

        public Builder setNamespace(final String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder setJobName(final String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder setTaskName(final String taskName) {
            this.taskName = taskName;
            return this;
        }

        public CloudWatchMetricConfiguration build() {
            return new CloudWatchMetricConfiguration(
                    batchSize,
                    includeTaskDimensions,
                    parseMetricName,
                    extractTaskManagerDimension,
                    extractOperatorDimension,
                    namespace,
                    jobName,
                    taskName);
        }
    }
}
