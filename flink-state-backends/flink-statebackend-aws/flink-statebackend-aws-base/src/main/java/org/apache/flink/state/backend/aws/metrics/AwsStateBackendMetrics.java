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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * Base metrics class for AWS state backends.
 *
 * <p>This class provides common metrics for monitoring the performance and behavior of AWS state backends.
 * It tracks operation counts, latencies, data sizes, and error rates for key operations.
 */
public abstract class AwsStateBackendMetrics {

    private final Counter numReads;
    private final Counter numWrites;
    private final Counter numDeletes;
    private final Counter bytesRead;
    private final Counter bytesWritten;
    private final Counter numErrors;

    private final Histogram readLatency;
    private final Histogram writeLatency;
    private final Histogram deleteLatency;

    /**
     * Creates a new AwsStateBackendMetrics instance.
     *
     * @param metricGroup The metric group to register metrics with
     * @param serviceName The AWS service name (e.g., "s3", "dynamodb")
     */
    protected AwsStateBackendMetrics(final MetricGroup metricGroup, final String serviceName) {
        final MetricGroup awsGroup = metricGroup.addGroup("aws").addGroup(serviceName);

        // Operation counters
        this.numReads = awsGroup.counter("numReads");
        this.numWrites = awsGroup.counter("numWrites");
        this.numDeletes = awsGroup.counter("numDeletes");

        // Data size counters
        this.bytesRead = awsGroup.counter("bytesRead");
        this.bytesWritten = awsGroup.counter("bytesWritten");

        // Error counter
        this.numErrors = awsGroup.counter("numErrors");

        // Latency histograms (using 100 samples)
        this.readLatency = awsGroup.histogram(
                "readLatency",
                new DescriptiveStatisticsHistogram(100));
        this.writeLatency = awsGroup.histogram(
                "writeLatency",
                new DescriptiveStatisticsHistogram(100));
        this.deleteLatency = awsGroup.histogram(
                "deleteLatency",
                new DescriptiveStatisticsHistogram(100));
    }

    // Read metrics
    public void reportRead(final long bytes, final long latencyMs) {
        numReads.inc();
        bytesRead.inc(bytes);
        readLatency.update(latencyMs);
    }

    // Write metrics
    public void reportWrite(final long bytes, final long latencyMs) {
        numWrites.inc();
        bytesWritten.inc(bytes);
        writeLatency.update(latencyMs);
    }

    // Delete metrics
    public void reportDelete(final long latencyMs) {
        numDeletes.inc();
        deleteLatency.update(latencyMs);
    }

    // Error metrics
    public void reportError() {
        numErrors.inc();
    }
}
