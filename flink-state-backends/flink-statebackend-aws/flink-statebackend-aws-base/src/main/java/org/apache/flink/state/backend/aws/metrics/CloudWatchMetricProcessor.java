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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.util.Preconditions;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class for processing metrics and converting them to CloudWatch format.
 *
 * <p>This class handles the creation of MetricDatum objects with appropriate
 * dimensions and validation. It also tracks metrics for batch processing.
 */
@Internal
public class CloudWatchMetricProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMetricProcessor.class);

    private final String originalMetricName;
    private final String processedMetricName;
    private final List<Dimension> dimensions;
    private final List<MetricDatum> metricData;
    private final CloudWatchMetricConfiguration config;

    /**
     * Create a new metric processor for a specific metric.
     *
     * @param originalName The original metric name
     * @param baseDimensions The base dimensions to include with all metrics
     * @param config The CloudWatch metric configuration
     */
    public CloudWatchMetricProcessor(
            final String originalName,
            final List<Dimension> baseDimensions,
            final CloudWatchMetricConfiguration config) {
        this.originalMetricName = originalName;
        // Use Preconditions to validate inputs
        this.dimensions = new ArrayList<>(Preconditions.checkNotNull(baseDimensions, "Base dimensions cannot be null"));
        this.config = Preconditions.checkNotNull(config, "Configuration cannot be null");
        this.metricData = new ArrayList<>();

        // Process the metric name to extract standard dimensions (TaskManager ID, Operator name)
        String parsedMetricName = CloudWatchMetricUtils.parseMetricName(
                originalName,
                dimensions,
                config.getJobName(),
                config.isParseMetricName(),
                config.isExtractTaskManagerDimension(),
                config.isExtractOperatorDimension());

        // Always process AWS service components if metric name parsing is enabled
        if (config.isParseMetricName()) {
            this.processedMetricName = CloudWatchMetricUtils.extractAwsServiceComponents(
                    parsedMetricName,
                    dimensions);
        } else {
            this.processedMetricName = parsedMetricName;
        }

        LOG.trace("Created metric processor for '{}' (processed to '{}')",
                originalName, processedMetricName);
    }

    /**
     * Add a metric with the processed name.
     *
     * @param value The metric value
     * @param unit The metric unit
     * @return true if the metric was added, false if invalid
     */
    public boolean addMetric(final double value, final StandardUnit unit) {
        return addMetric(processedMetricName, value, unit);
    }

    /**
     * Add a metric with a custom suffix appended to the processed name.
     *
     * @param suffix The suffix to append (e.g., ".count", ".max")
     * @param value The metric value
     * @param unit The metric unit
     * @return true if the metric was added, false if invalid
     */
    public boolean addMetricWithSuffix(final String suffix, final double value, final StandardUnit unit) {
        String metricName;
        if (!suffix.startsWith(".") && !processedMetricName.endsWith(".")) {
            metricName = processedMetricName + "." + suffix;
        } else {
            metricName = processedMetricName + suffix;
        }
        return addMetric(metricName, value, unit);
    }

    /**
     * Add a metric with a custom name.
     *
     * @param name The custom metric name
     * @param value The metric value
     * @param unit The metric unit
     * @return true if the metric was added, false if invalid
     */
    public boolean addMetric(final String name, final double value, final StandardUnit unit) {
        // Skip invalid values
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            LOG.trace("Skipping metric '{}' with invalid value: {}", name, value);
            return false;
        }

        final MetricDatum datum = MetricDatum.builder()
                .metricName(name)
                .value(value)
                .unit(unit)
                .dimensions(dimensions)
                .timestamp(Instant.now())
                .build();

        metricData.add(datum);

        return true;
    }

    /**
     * Get the processed metric name.
     *
     * @return The processed metric name
     */
    public String getMetricName() {
        return processedMetricName;
    }

    /**
     * Get the original metric name.
     *
     * @return The original metric name
     */
    public String getOriginalMetricName() {
        return originalMetricName;
    }

    /**
     * Get the list of collected metric data.
     *
     * @return An unmodifiable list of metric data
     */
    public List<MetricDatum> getMetricData() {
        return Collections.unmodifiableList(metricData);
    }

    /**
     * Clear the collected metric data.
     */
    public void clearMetricData() {
        metricData.clear();
    }

    /**
     * Get the number of metrics collected.
     *
     * @return The number of metrics
     */
    public int getMetricCount() {
        return metricData.size();
    }
}
