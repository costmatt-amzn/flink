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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options for the CloudWatch metrics reporter.
 *
 * <p>These options can be configured in the Flink configuration to customize
 * the behavior of the CloudWatch metrics reporter.
 */
@PublicEvolving
public final class CloudWatchOptions {

    /** Base prefix for CloudWatch metrics reporter configuration. */
    public static final String CLOUDWATCH_REPORTER_PREFIX = "metrics.reporter.cloudwatch";
    /**
     * The CloudWatch namespace to use for reported metrics.
     *
     * <p>CloudWatch organizes metrics into namespaces. This option specifies
     * the namespace where Flink metrics will be published.
     *
     * <p>Default value: "Apache/Flink"
     */
    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".namespace")
                    .stringType()
                    .defaultValue("Apache/Flink")
                    .withDescription("The CloudWatch namespace to use for reported metrics.");
    /**
     * Custom prefix for the AWS SDK user agent when making CloudWatch API calls.
     *
     * <p>This option allows customizing the user agent string that the AWS SDK
     * will use when sending requests to CloudWatch.
     */
    public static final ConfigOption<String> USER_AGENT_PREFIX =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".user-agent-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom prefix for the AWS SDK user agent when making CloudWatch API calls.");
    /**
     * The interval between metric reports in milliseconds.
     *
     * <p>This option controls how frequently metrics are sent to CloudWatch.
     * Lower values provide more timely data but increase API calls and costs.
     *
     * <p>Default value: 60000 (1 minute)
     */
    public static final ConfigOption<Long> REPORT_INTERVAL =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".interval")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("The interval between metric reports in milliseconds.");
    /**
     * Whether to include dimensions for task metrics.
     *
     * <p>When enabled, additional dimensions like TaskManager ID and Task name
     * will be included with metrics. This provides more detailed metrics but
     * increases the number of CloudWatch metrics created.
     *
     * <p>Default value: false
     */
    public static final ConfigOption<Boolean> INCLUDE_TASK_DIMENSIONS =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".include-task-dimensions")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to include dimensions for task metrics.");

    /**
     * Whether to parse metric names to extract additional dimensions.
     *
     * <p>When enabled, the reporter will extract components like TaskManager ID and operator name
     * as dimensions. This improves filtering and grouping capabilities in CloudWatch.
     *
     * <p>Example: A metric named ".taskmanager.id.job.operator.metric" becomes "metric" with
     * dimensions for TaskManagerId and OperatorName.
     *
     * <p>Note: This may increase the number of unique metric/dimension combinations in CloudWatch,
     * which could affect CloudWatch costs.
     *
     * <p>Default value: true
     */
    public static final ConfigOption<Boolean> PARSE_METRIC_NAME =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".parse-metric-name")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to parse metric names to extract additional dimensions.");

    /**
     * Whether to extract the TaskManager ID as a dimension.
     *
     * <p>When enabled, the TaskManager ID will be extracted from the metric name
     * and added as a "TaskManagerId" dimension. Only effective when parse-metric-name is true.
     *
     * <p>Default value: true
     */
    public static final ConfigOption<Boolean> EXTRACT_TASKMANAGER_DIMENSION =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".extract-taskmanager-dimension")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to extract the TaskManager ID as a CloudWatch dimension.");

    /**
     * Whether to extract the operator name as a dimension.
     *
     * <p>When enabled, the operator name will be extracted from the metric name
     * and added as an "OperatorName" dimension. Only effective when parse-metric-name is true.
     *
     * <p>Default value: true
     */
    public static final ConfigOption<Boolean> EXTRACT_OPERATOR_DIMENSION =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".extract-operator-dimension")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to extract the operator name as a CloudWatch dimension.");
    /**
     * Maximum number of metrics to send in a single CloudWatch API request.
     *
     * <p>CloudWatch has a limit of 20 metrics per request. This option should generally
     * not be changed unless you have specific throttling needs.
     *
     * <p>Default value: 20
     */
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".batch-size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Maximum number of metrics to send in a single CloudWatch API request.");

    /**
     * The job name to use as a CloudWatch dimension.
     *
     * <p>This option specifies the job name that will be used as a dimension in CloudWatch metrics.
     * If not specified, the value from metrics.scope.job.name will be used.
     *
     * <p>Default value: "undefined"
     */
    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".job-name")
                    .stringType()
                    .defaultValue("undefined")
                    .withDescription("The job name to use as a CloudWatch dimension.");

    /**
     * The task name to use as a CloudWatch dimension.
     *
     * <p>This option specifies the task name that will be used as a dimension in CloudWatch metrics
     * when include-task-dimensions is true. If not specified, the value from metrics.scope.task.name will be used.
     *
     * <p>Default value: "undefined"
     */
    public static final ConfigOption<String> TASK_NAME =
            ConfigOptions.key(CLOUDWATCH_REPORTER_PREFIX + ".task-name")
                    .stringType()
                    .defaultValue("undefined")
                    .withDescription("The task name to use as a CloudWatch dimension when include-task-dimensions is true.");

}
