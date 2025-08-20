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
 * Base metrics class for AWS snapshot operations.
 *
 * <p>This class provides common metrics for monitoring the performance and behavior of AWS snapshot operations.
 * It tracks checkpoint operations, latencies, data sizes, and error rates.
 */
public abstract class AwsSnapshotMetrics {

    private final Counter numCheckpoints;
    private final Counter numCheckpointCompletes;
    private final Counter numCheckpointAborts;
    private final Counter bytesUploaded;
    private final Counter numErrors;

    private final Histogram checkpointLatency;

    /**
     * Creates a new AwsSnapshotMetrics instance.
     *
     * @param metricGroup The metric group to register metrics with
     * @param serviceName The AWS service name (e.g., "s3", "dynamodb")
     */
    protected AwsSnapshotMetrics(final MetricGroup metricGroup, final String serviceName) {
        final MetricGroup snapshotGroup = metricGroup
                .addGroup("aws")
                .addGroup(serviceName)
                .addGroup("snapshot");

        // Operation counters
        this.numCheckpoints = snapshotGroup.counter("numCheckpoints");
        this.numCheckpointCompletes = snapshotGroup.counter("numCheckpointCompletes");
        this.numCheckpointAborts = snapshotGroup.counter("numCheckpointAborts");

        // Data size counter
        this.bytesUploaded = snapshotGroup.counter("bytesUploaded");

        // Error counter
        this.numErrors = snapshotGroup.counter("numErrors");

        // Latency histograms
        this.checkpointLatency = snapshotGroup.histogram(
                "checkpointLatency",
                new DescriptiveStatisticsHistogram(100));
    }

    // Checkpoint metrics
    public void reportCheckpoint(final long bytes, final long latencyMs) {
        numCheckpoints.inc();
        bytesUploaded.inc(bytes);
        checkpointLatency.update(latencyMs);
    }

    // Checkpoint complete metrics
    public void reportCheckpointComplete() {
        numCheckpointCompletes.inc();
    }

    // Checkpoint abort metrics
    public void reportCheckpointAbort(final long latencyMs) {
        numCheckpointAborts.inc();
    }

    // Error metrics
    public void reportError() {
        numErrors.inc();
    }
}
