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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.util.Preconditions;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;

import java.util.List;

/**
 * Utility class for CloudWatch metric operations.
 *
 * <p>This class provides helper methods for CloudWatch metric management,
 * including dimension creation, metric name parsing, and validation according
 * to CloudWatch constraints.
 */
public class CloudWatchMetricUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMetricUtils.class);

    // Constants for CloudWatch constraints
    public static final String TASKMANAGER_PREFIX = ".taskmanager.";
    public static final int MAX_CLOUDWATCH_DIMENSIONS = 30;
    public static final int MAX_DIMENSION_VALUE_LENGTH = 255;

    // Dimension names
    public static final String DIMENSION_TASKMANAGER_ID = "TaskManagerId";
    public static final String DIMENSION_OPERATOR_NAME = "OperatorName";
    public static final String DIMENSION_JOB_NAME = "JobName";
    public static final String DIMENSION_TASK_NAME = "TaskName";
    public static final String DIMENSION_SERVICE_TYPE = "ServiceType";
    public static final String DIMENSION_RESOURCE_TYPE = "ResourceType";
    public static final String DIMENSION_OPERATION = "Operation";
    public static final String DIMENSION_METRIC_TYPE = "MetricType";

    /**
     * Creates a CloudWatch dimension with validated name and value.
     *
     * @param name The dimension name
     * @param value The dimension value
     * @return A properly constructed CloudWatch dimension, or null if invalid
     */
    public static Dimension createDimension(final String name, final String value) {
        if (StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
            return null;
        }

        // Truncate values that exceed CloudWatch limits using StringUtils
        final String validatedName = StringUtils.substring(name, 0, MAX_DIMENSION_VALUE_LENGTH);
        final String validatedValue = StringUtils.substring(value, 0, MAX_DIMENSION_VALUE_LENGTH);

        if (name.length() > MAX_DIMENSION_VALUE_LENGTH || value.length() > MAX_DIMENSION_VALUE_LENGTH) {
            LOG.warn("Dimension or value exceeded CloudWatch limit of {} characters and was truncated",
                    MAX_DIMENSION_VALUE_LENGTH);
        }

        return Dimension.builder()
                .name(validatedName)
                .value(validatedValue)
                .build();
    }

    /**
     * Helper method to add a dimension to the list if it's valid and under the limit.
     *
     * @param dimensions The list to add the dimension to
     * @param dimension The dimension to add
     * @return true if the dimension was added, false otherwise
     */
    public static boolean addDimensionIfValid(final List<Dimension> dimensions, final Dimension dimension) {
        // Use Guava Preconditions for dimensions list validation
        Preconditions.checkNotNull(dimensions, "Dimensions list cannot be null");

        if (dimension != null && dimensions.size() < MAX_CLOUDWATCH_DIMENSIONS) {
            dimensions.add(dimension);
            return true;
        }
        return false;
    }

    /**
     * Parses a metric name to extract dimensions like TaskManager ID and operator name.
     * Example: ".taskmanager.cee60705-369b-462c-abf2-6e76190b04ab.PPA Streaming Job.BalanceDateWindowFunction.9.aws.s3.writeLatency.max"
     *
     * @param fullMetricName The full metric name to parse
     * @param dimensions The list of dimensions to add the extracted dimensions to
     * @param jobName The job name to help identify operator part
     * @param parseMetricName Whether to parse the metric name for dimensions
     * @param extractTaskManagerDimension Whether to extract the TaskManager ID
     * @param extractOperatorDimension Whether to extract the operator name
     * @return The simplified metric name after removing extracted components
     */
    public static String parseMetricName(
            final String fullMetricName,
            final List<Dimension> dimensions,
            final String jobName,
            final boolean parseMetricName,
            final boolean extractTaskManagerDimension,
            final boolean extractOperatorDimension) {

        // Quick validation checks with StringUtils
        if (!parseMetricName || StringUtils.isEmpty(fullMetricName) ||
                dimensions.size() >= MAX_CLOUDWATCH_DIMENSIONS) {
            return fullMetricName;
        }

        LOG.trace("Parsing metric name: {}", fullMetricName);
        String metricName = fullMetricName;

        try {
            // Extract TaskManager ID if configured
            if (extractTaskManagerDimension && metricName.startsWith(TASKMANAGER_PREFIX)) {
                metricName = extractTaskManagerId(metricName, dimensions);
            }

            // Extract operator name if configured
            if (extractOperatorDimension && metricName.contains(".") &&
                    dimensions.size() < MAX_CLOUDWATCH_DIMENSIONS) {
                metricName = extractOperatorName(metricName, dimensions, jobName);
            }

            // Clean up the remaining metric name by trimming leading dots
            if (metricName.startsWith(".")) {
                metricName = metricName.substring(1);
            }

            LOG.trace("Simplified metric name: {}", metricName);
            return metricName;

        } catch (final Exception e) {
            LOG.warn("Error parsing metric name '{}': {}", fullMetricName, e.getMessage());
            return fullMetricName;
        }
    }

    /**
     * Extracts the TaskManager ID from a metric name.
     *
     * @param metricName The metric name to extract from
     * @param dimensions The list to add the dimension to
     * @return The updated metric name with TaskManager part removed
     */
    public static String extractTaskManagerId(final String metricName, final List<Dimension> dimensions) {
        final int startIdx = TASKMANAGER_PREFIX.length();
        final int endIdx = metricName.indexOf('.', startIdx);

        if (endIdx > startIdx) {
            final String taskManagerId = metricName.substring(startIdx, endIdx);
            if (!StringUtils.isEmpty(taskManagerId)) {
                addDimensionIfValid(dimensions, createDimension(DIMENSION_TASKMANAGER_ID, taskManagerId));
                return metricName.substring(endIdx);
            }
        }

        return metricName;
    }

    /**
     * Extracts the operator name from a metric name.
     *
     * @param metricName The metric name to extract from
     * @param dimensions The list to add the dimension to
     * @param jobName The job name to help identify operator part
     * @return The updated metric name with operator part removed
     */
    public static String extractOperatorName(final String metricName, final List<Dimension> dimensions, final String jobName) {
        // Skip the first dot if present
        final int startIdx = metricName.startsWith(".") ? 1 : 0;

        final String[] segments = metricName.substring(startIdx).split("\\.");
        if (segments.length < 2) {
            return metricName;
        }

        // Identify operator segment index
        int operatorIdx = 0;
        if (segments.length > 1 && segments[0].equals(jobName)) {
            operatorIdx = 1;
        }

        if (segments.length <= operatorIdx) {
            return metricName;
        }

        // Build operator name (include number segment if present)
        final StringBuilder operatorBuilder = new StringBuilder(segments[operatorIdx]);
        final boolean hasNumericPart = segments.length > operatorIdx + 1 && isNumeric(segments[operatorIdx + 1]);
        if (hasNumericPart) {
            operatorBuilder.append(".").append(segments[operatorIdx + 1]);
        }

        final String operatorName = operatorBuilder.toString();
        if (!operatorName.isEmpty()) {
            addDimensionIfValid(dimensions, createDimension(DIMENSION_OPERATOR_NAME, operatorName));

            // Calculate the position to trim from
            int trimPosition = startIdx;
            for (int i = 0; i <= operatorIdx + (hasNumericPart ? 1 : 0); i++) {
                trimPosition += segments[i].length() + 1; // +1 for the dot
            }
            return metricName.substring(Math.min(trimPosition, metricName.length()));
        }

        return metricName;
    }

    /**
     * Checks if a string represents a numeric value.
     *
     * @param str The string to check
     * @return true if the string represents a numeric value, false otherwise
     */
    public static boolean isNumeric(final String str) {
        // Use Apache Commons StringUtils for numeric check
        return StringUtils.isNumeric(str);
    }

    /**
     * Check if a string is a recognized statistic type.
     *
     * @param segment The string to check
     * @return true if it's a recognized statistic type, false otherwise
     */
    public static boolean isStatisticType(final String segment) {
        if (StringUtils.isEmpty(segment)) {
            return false;
        }
        final String lower = segment.toLowerCase();
        return lower.equals("min") || lower.equals("max") || lower.equals("avg") ||
               lower.equals("sum") || lower.equals("count") || lower.matches("p\\d+");
    }

    /**
     * Extracts AWS service components from metric names with patterns like 'aws.s3.writeLatency.min'.
     * Always extracts service, resource, and operation components as dimensions.
     *
     * @param metricName The metric name to extract components from
     * @param dimensions The list of dimensions to add the extracted dimensions to
     * @return The updated metric name with extracted components removed
     */
    public static String extractAwsServiceComponents(
            final String metricName,
            final List<Dimension> dimensions) {

        if (StringUtils.isEmpty(metricName) || dimensions.size() >= MAX_CLOUDWATCH_DIMENSIONS) {
            return metricName;
        }

        final String[] segments = metricName.split("\\.");
        if (segments.length < 3) {
            return metricName;
        }

        int currentIdx = 0;
        String baseMetricName = metricName;

        // Extract service type (e.g., 'aws')
        if (currentIdx < segments.length) {
            String serviceType = segments[currentIdx];
            addDimensionIfValid(dimensions, createDimension(DIMENSION_SERVICE_TYPE, serviceType));
            currentIdx++;
        }

        // Extract resource type (e.g., 's3')
        if (currentIdx < segments.length) {
            String resourceType = segments[currentIdx];
            addDimensionIfValid(dimensions, createDimension(DIMENSION_RESOURCE_TYPE, resourceType));
            currentIdx++;
        }

        // Extract operation and metric type (e.g., 'writeLatency')
        if (currentIdx < segments.length) {
            String combinedOperation = segments[currentIdx];

            // Try to extract operation from combined field
            for (String opType : new String[]{"write", "read", "delete", "get", "put", "compression", "decompression", "compress", "decompress"}) {
                if (combinedOperation.toLowerCase().startsWith(opType.toLowerCase())) {
                    addDimensionIfValid(dimensions, createDimension(DIMENSION_OPERATION, opType));

                    // Use 'Latency' as the base metric name if available
                    if (combinedOperation.toLowerCase().endsWith("latency")) {
                        baseMetricName = "Latency";
                        addDimensionIfValid(dimensions, createDimension(DIMENSION_METRIC_TYPE, "Latency"));
                    } else {
                        // If not ending with latency, use remaining part after operation
                        String metricType = combinedOperation.substring(opType.length());
                        if (!StringUtils.isEmpty(metricType)) {
                            baseMetricName = metricType;
                            addDimensionIfValid(dimensions, createDimension(DIMENSION_METRIC_TYPE, metricType));
                        } else {
                            baseMetricName = opType; // Use operation as metric name if no type
                        }
                    }

                    currentIdx++;
                    break;
                }
            }
        }

        // Always handle statistic suffixes
        if (segments.length > currentIdx) {
            String lastSegment = segments[segments.length - 1];
            if (isStatisticType(lastSegment)) {
                // Use the metric name without the statistic suffix
                if (segments.length > currentIdx + 1) {
                    baseMetricName = String.join(".",
                        java.util.Arrays.copyOfRange(segments, currentIdx, segments.length - 1));
                }
            } else {
                // Keep full remaining path after extracted dimensions (if needed)
                if (segments.length > currentIdx && !baseMetricName.equals("Latency")) {
                    baseMetricName = String.join(".",
                        java.util.Arrays.copyOfRange(segments, currentIdx, segments.length));
                }
            }
        }

        return baseMetricName;
    }

    private CloudWatchMetricUtils() {
        // Utility class should not be instantiated
    }
}
