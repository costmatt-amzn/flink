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

package org.apache.flink.state.backend.aws.s3.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.state.backend.aws.metrics.AwsStateBackendMetrics;

/**
 * Metrics for the S3 state backend.
 *
 * <p>This class provides metrics for monitoring the performance and behavior of the S3 state backend.
 * It extends the base AWS metrics with S3-specific metrics like compression/decompression latencies.
 */
public class S3StateBackendMetrics extends AwsStateBackendMetrics {

    private final Histogram compressionLatency;
    private final Histogram decompressionLatency;

    /**
     * Creates a new S3StateBackendMetrics instance.
     *
     * @param metricGroup The metric group to register metrics with
     */
    public S3StateBackendMetrics(MetricGroup metricGroup) {
        super(metricGroup, "s3");

        MetricGroup s3Group = metricGroup.addGroup("aws").addGroup("s3");

        // S3-specific latency histograms
        this.compressionLatency = s3Group.histogram("compressionLatency", new DescriptiveStatisticsHistogram(100));
        this.decompressionLatency = s3Group.histogram("decompressionLatency", new DescriptiveStatisticsHistogram(100));
    }

    // Compression metrics
    public void reportCompression(long latencyMs) {
        compressionLatency.update(latencyMs);
    }

    // Decompression metrics
    public void reportDecompression(long latencyMs) {
        decompressionLatency.update(latencyMs);
    }
}