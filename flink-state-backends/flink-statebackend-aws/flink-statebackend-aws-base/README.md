# Flink AWS State Backend Base

This module provides the common foundation for AWS-based state backends in Apache Flink. It contains shared abstractions, utilities, and base classes that are used by the service-specific state backend implementations.

## Overview

The base module includes:

- Abstract base classes for AWS state backends
- Common configuration handling for AWS services
- Serialization utilities for state data
- Snapshot and checkpoint strategies
- AWS client management and configuration

## Key Components

### CloudWatchMetricReporter

The `CloudWatchMetricReporter` class provides a Flink metrics reporter that exports metrics to Amazon CloudWatch. It:

- Collects metrics from Flink's metrics system
- Reports metrics to CloudWatch at configurable intervals
- Supports counters, gauges, histograms, and meters
- Provides configurable dimensions for better metric organization

### AbstractAwsStateBackend

The `AbstractAwsStateBackend` class serves as the foundation for all AWS-based state backends. It:

- Manages AWS client creation and configuration
- Implements common state backend methods
- Provides a framework for service-specific implementations

### AwsKeyedStateBackendBase

The `AwsKeyedStateBackendBase` class provides a base implementation for keyed state backends that use AWS services. It:

- Handles state registration and serialization
- Manages key contexts and namespaces
- Implements common keyed state operations
- Provides abstract methods for service-specific storage operations

### AbstractAwsKeyedStateBackendBuilder

The `AbstractAwsKeyedStateBackendBuilder` class provides a builder pattern for creating AWS-based keyed state backends. It:

- Manages the creation of key contexts and serializers
- Handles state restoration from checkpoints
- Provides a framework for service-specific builder implementations

### AbstractAwsStateBackendFactory

The `AbstractAwsStateBackendFactory` class implements the Flink `StateBackendFactory` interface for AWS-based state backends. It:

- Creates state backends from configuration
- Validates required configuration parameters
- Provides a framework for service-specific factory implementations

## Configuration

The base module defines common configuration options for AWS services:

```java
// AWS region configuration
config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");

// AWS credentials configuration
config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");

// AWS endpoint configuration (for testing or custom endpoints)
config.set(AWSConfigOptions.AWS_ENDPOINT_OPTION, "http://localhost:4566");
```

### CloudWatch Metrics Reporter Configuration

The module also includes a CloudWatch metrics reporter that can be configured in your Flink configuration:

```java
// Configure CloudWatch metrics reporter
config.setString("metrics.reporter.cloudwatch.factory.class", 
        "org.apache.flink.state.backend.aws.metrics.CloudWatchMetricReporterFactory");
config.set(CloudWatchOptions.NAMESPACE, "Apache/Flink");
config.set(CloudWatchOptions.REPORT_INTERVAL, 60000L);  // Report every 60 seconds
config.set(CloudWatchOptions.INCLUDE_TASK_DIMENSIONS, true);
```

Or in your `flink-conf.yaml`:

```yaml
# Enable CloudWatch metrics reporter
metrics.reporter.cloudwatch.factory.class: org.apache.flink.state.backend.aws.metrics.CloudWatchMetricReporterFactory
metrics.reporter.cloudwatch.namespace: Apache/Flink
metrics.reporter.cloudwatch.interval: 60000
metrics.reporter.cloudwatch.include-task-dimensions: true

# AWS configuration required for CloudWatch access
aws.region: us-west-2
aws.credentials.provider: BASIC
aws.credentials.provider.basic.accesskeyid: your-access-key
aws.credentials.provider.basic.secretkey: your-secret-key
```

## Extension Points

To create a new AWS-based state backend, you need to extend the following classes:

1. `AbstractAwsStateBackend` - Implement service-specific client creation and state backend operations
2. `AwsKeyedStateBackendBase` - Implement service-specific state storage and retrieval operations
3. `AbstractAwsKeyedStateBackendBuilder` - Implement service-specific builder logic
4. `AbstractAwsStateBackendFactory` - Implement service-specific factory logic

## Usage

This module is not used directly but serves as a dependency for the service-specific state backend implementations:

- `flink-statebackend-aws-dynamodb` - DynamoDB-based state backend
- `flink-statebackend-aws-s3` - S3-based state backend

## Dependencies

- Apache Flink Core
- AWS SDK for Java v2.x
- SLF4J for logging

## License

This module is licensed under the Apache License 2.0.
