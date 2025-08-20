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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.state.backend.aws.config.CloudWatchOptions;
import org.apache.flink.state.backend.aws.util.AWSClientUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * Reporter that exports Flink metrics to Amazon CloudWatch.
 *
 * <p>This reporter collects metrics from Flink's metric system and reports them to CloudWatch
 * at regular intervals. It supports counters, gauges, histograms, and meters.
 */
@PublicEvolving
public class CloudWatchMetricReporter extends AbstractReporter implements Scheduled, CharacterFilter {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMetricReporter.class);

    // Configuration
    private CloudWatchMetricConfiguration config;

    // AWS client
    private final CloudWatchClient cloudWatchClient;

    /**
     * Creates a new CloudWatchMetricReporter.
     */
    public CloudWatchMetricReporter() {
        this(new Properties());
    }

    /**
     * Creates a new CloudWatchMetricReporter with the given properties.
     *
     * @param properties Configuration properties for the reporter
     */
    public CloudWatchMetricReporter(final Properties properties) {
        LOG.debug("CloudWatch metric reporter created with properties");

        // Initialize with default configuration
        this.config = CloudWatchMetricConfiguration.fromDefaults();

        // Initialize CloudWatch client
        final SdkHttpClient syncHttpClient = AWSClientUtil
                .createSyncHttpClient(properties, ApacheHttpClient.builder());

        this.cloudWatchClient = AWSClientUtil.createAwsSyncClient(
                properties,
                syncHttpClient,
                CloudWatchClient.builder(),
                "Flink-CloudWatch-MetricReporter/%s-%s",
                CloudWatchOptions.USER_AGENT_PREFIX.key());
    }

    @Override
    public void open(final MetricConfig metricConfig) {
        // Update configuration with values from MetricConfig
        this.config = config.withUpdatesFrom(metricConfig);

        LOG.info("CloudWatch metrics reporter opened with namespace: {}, job: {}, task: {}, parseMetricName: {}",
                config.getNamespace(), config.getJobName(), config.getTaskName(), config.isParseMetricName());
    }

    @Override
    public void close() {
        // Close the CloudWatch client
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
            LOG.info("CloudWatch metrics reporter closed");
        }
    }

    @Override
    public void report() {
        final long startTime = System.currentTimeMillis();
        int totalMetricsCollected = 0;

        try {
            LOG.debug("Starting metrics reporting cycle for namespace: {}", config.getNamespace());

            // Create base dimensions shared by all metrics
            final List<Dimension> baseDimensions = createBaseDimensions();
            final List<MetricDatum> batchedMetrics = new ArrayList<>();

            // Process each metric type using the template pattern
            totalMetricsCollected += processMetrics(counters, batchedMetrics, baseDimensions, this::processCounter);
            totalMetricsCollected += processMetrics(gauges, batchedMetrics, baseDimensions, this::processGauge);
            totalMetricsCollected += processMetrics(histograms, batchedMetrics, baseDimensions, this::processHistogram);
            totalMetricsCollected += processMetrics(meters, batchedMetrics, baseDimensions, this::processMeter);

            // Send any remaining metrics
            if (!batchedMetrics.isEmpty()) {
                LOG.debug("Sending final batch of {} metrics", batchedMetrics.size());
                sendMetricBatch(batchedMetrics);
            }

            final long endTime = System.currentTimeMillis();
            LOG.info("Metrics reporting cycle completed: {} metrics reported in {} ms",
                    totalMetricsCollected, (endTime - startTime));

        } catch (final AwsServiceException e) {
            LOG.error("AWS service error reporting metrics to CloudWatch: {}", e.getMessage(), e);
        } catch (final SdkClientException e) {
            LOG.error("AWS client error reporting metrics to CloudWatch: {}", e.getMessage(), e);
        } catch (final Exception e) {
            LOG.error("Unexpected error reporting metrics to CloudWatch", e);
        }
    }

    /**
     * Creates the base dimensions that will be included with all metrics.
     *
     * @return A list of base dimensions
     */
    private List<Dimension> createBaseDimensions() {
        final List<Dimension> dimensions = new ArrayList<>();

        // Add JobName dimension
        final Dimension jobDimension = CloudWatchMetricUtils.createDimension(
                CloudWatchMetricUtils.DIMENSION_JOB_NAME, config.getJobName());
        if (jobDimension != null) {
            dimensions.add(jobDimension);
        } else {
            LOG.warn("Invalid JobName dimension value, using default");
            dimensions.add(CloudWatchMetricUtils.createDimension(
                    CloudWatchMetricUtils.DIMENSION_JOB_NAME, "undefined"));
        }

        // Add TaskName dimension if configured
        if (config.isIncludeTaskDimensions()) {
            final Dimension taskDimension = CloudWatchMetricUtils.createDimension(
                    CloudWatchMetricUtils.DIMENSION_TASK_NAME, config.getTaskName());
            if (taskDimension != null) {
                dimensions.add(taskDimension);
                LOG.debug("Including task dimensions with TaskName: {}", config.getTaskName());
            } else {
                LOG.warn("Invalid TaskName dimension value, skipping dimension");
            }
        }

        return dimensions;
    }

    /**
     * Generic method to process metrics of any type.
     *
     * @param <T> The metric type
     * @param metrics The metrics to process
     * @param batchedMetrics The list to add processed metrics to
     * @param baseDimensions The base dimensions to include with all metrics
     * @param processor The function to process each metric
     * @return The number of metrics processed
     */
    private <T> int processMetrics(
            final Map<T, String> metrics,
            final List<MetricDatum> batchedMetrics,
            final List<Dimension> baseDimensions,
            final BiConsumer<Map.Entry<T, String>, CloudWatchMetricProcessor> processor) {

        synchronized (this) {
            if (metrics.isEmpty()) {
                LOG.debug("No metrics to process");
                return 0;
            }

            String metricType = metrics.values().iterator().next().getClass().getSimpleName();
            LOG.debug("Processing {} {}", metrics.size(), metricType);

            // Use Java Streams to process metrics
            return metrics.entrySet().stream()
                .mapToInt(entry -> {
                    final CloudWatchMetricProcessor metricProcessor = new CloudWatchMetricProcessor(
                            entry.getValue(), baseDimensions, config);

                    // Let the specific processor handle this metric
                    processor.accept(entry, metricProcessor);

                    // Add the processed metrics to our batch
                    if (metricProcessor.getMetricCount() > 0) {
                        batchedMetrics.addAll(metricProcessor.getMetricData());

                        // Send if we've reached the batch size
                        if (batchedMetrics.size() >= config.getBatchSize()) {
                            LOG.debug("Sending batch of {} metrics", batchedMetrics.size());
                            sendMetricBatch(batchedMetrics);
                            batchedMetrics.clear();
                        }

                        return metricProcessor.getMetricCount();
                    }

                    return 0;
                })
                .sum();
        }
    }

    /**
     * Process a counter metric.
     */
    private void processCounter(final Map.Entry<Counter, String> entry, final CloudWatchMetricProcessor processor) {
        final long count = entry.getKey().getCount();
        LOG.trace("Processing counter: {}={}", processor.getMetricName(), count);
        processor.addMetric(count, StandardUnit.COUNT);
    }

    /**
     * Process a gauge metric.
     */
    private void processGauge(final Map.Entry<Gauge<?>, String> entry, final CloudWatchMetricProcessor processor) {
        final Object value = entry.getKey().getValue();

        if (value instanceof Number) {
            final double numericValue = ((Number) value).doubleValue();
            LOG.trace("Processing gauge: {}={}", processor.getMetricName(), numericValue);
            processor.addMetric(numericValue, StandardUnit.NONE);
        } else {
            LOG.trace("Skipping non-numeric gauge: {} with value type {}",
                    processor.getOriginalMetricName(),
                    value != null ? value.getClass().getSimpleName() : "null");
        }
    }

    /**
     * Process a histogram metric.
     */
    /**
     * Process a histogram metric using CloudWatch's native statistics functionality.
     * Instead of sending pre-calculated statistics, we send the raw values and let
     * CloudWatch calculate statistics like min, max, avg, p99, etc.
     */
    private void processHistogram(final Map.Entry<Histogram, String> entry, final CloudWatchMetricProcessor processor) {
        final Histogram histogram = entry.getKey();
        LOG.trace("Processing histogram: {}", processor.getMetricName());

        // Send raw individual values instead of pre-calculated statistics
        // CloudWatch will calculate min, max, avg, p99, etc. from these values
        for (long value : histogram.getStatistics().getValues()) {
            processor.addMetric((double) value, StandardUnit.NONE);
        }
    }

    /**
     * Process a meter metric using CloudWatch's native statistics.
     * For meters, we send the raw count and use CloudWatch to calculate rates.
     */
    private void processMeter(final Map.Entry<Meter, String> entry, final CloudWatchMetricProcessor processor) {
        final Meter meter = entry.getKey();
        LOG.trace("Processing meter: {}", processor.getMetricName());

        // For count, we still use a separate metric with COUNT unit
        // This will help distinguish it from other statistics
        processor.addMetricWithSuffix(".count", meter.getCount(), StandardUnit.COUNT);

        // For rate, we can just use the raw count and let CloudWatch calculate the rate using SampleCount
        // over the reporting interval, which is equivalent to a rate
        processor.addMetricWithSuffix(".value", meter.getCount(), StandardUnit.NONE);
    }

    /**
     * Send a batch of metrics to CloudWatch.
     *
     * @param metrics The metrics to send
     */
    private void sendMetricBatch(final List<MetricDatum> metrics) {
        try {
            final long startTime = System.currentTimeMillis();

            // Use the AWS SDK's fluent request style
            LOG.debug("Sending {} metrics to CloudWatch namespace '{}'", metrics.size(), config.getNamespace());
            cloudWatchClient.putMetricData(req -> req
                    .namespace(config.getNamespace())
                    .metricData(metrics));

            final long endTime = System.currentTimeMillis();
            LOG.debug("CloudWatch putMetricData completed in {} ms", (endTime - startTime));
        } catch (final Exception e) {
            LOG.error("Failed to send metrics to CloudWatch: {}", e.getMessage(), e);
            // Don't rethrow - we want to continue reporting other metrics
        }
    }

    /**
     * Implements CharacterFilter interface to allow proper metric name filtering.
     * This method does not modify the input string.
     */
    @Override
    public String filterCharacters(final String input) {
        // No character filtering needed for CloudWatch metrics
        return input;
    }
}
