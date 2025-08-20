# Flink S3 State Backend

This module provides a state backend implementation for Apache Flink that uses Amazon S3 for state storage. It enables Flink applications to store their state in a highly durable, scalable, and cost-effective object storage service.

## Overview

The S3 state backend:

- Stores keyed state in S3 objects
- Supports configurable paths and prefixes
- Provides compression and encryption options
- Supports all standard Flink keyed state types
- Optimized for large state sizes

## Key Components

### S3StateBackend

The `S3StateBackend` class is the main entry point for using S3 as a state backend. It:

- Creates and configures S3 clients
- Manages S3 bucket access and validation
- Creates keyed state backends for operators
- Handles configuration and customization

### S3KeyedStateBackend

The `S3KeyedStateBackend` class implements the keyed state backend interface for S3. It:

- Stores and retrieves state data in S3 objects
- Manages serialization and deserialization of state
- Implements state iteration and querying
- Handles checkpoints and savepoints
- Provides compression and encryption support

### S3KeyedStateBackendBuilder

The `S3KeyedStateBackendBuilder` class provides a builder pattern for creating S3 keyed state backends. It:

- Configures the keyed state backend with appropriate settings
- Sets up snapshot strategies for checkpoints
- Handles state restoration from previous checkpoints

### S3StateBackendFactory

The `S3StateBackendFactory` class implements the Flink `StateBackendFactory` interface for the S3 state backend. It:

- Creates S3 state backends from configuration
- Validates required configuration parameters
- Enables the state backend to be used via configuration

## Configuration

### Required Configuration

```java
// AWS region configuration
config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");

// AWS credentials configuration
config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");

// S3 bucket configuration
config.set(S3StateBackendOptions.BUCKET_NAME, "flink-state");
config.set(S3StateBackendOptions.BASE_PATH, "my-job/state");
```

### Optional Configuration

```java
// State file options
config.set(S3StateBackendOptions.STATE_FILE_COMPRESSION, true);
config.set(S3StateBackendOptions.STATE_FILE_ENCRYPTION, true);
config.set(S3StateBackendOptions.STATE_FILE_EXTENSION, ".state");

// Path prefixes
config.set(S3StateBackendOptions.STATE_PREFIX, "state");
config.set(S3StateBackendOptions.CHECKPOINT_PREFIX, "checkpoints");
config.set(S3StateBackendOptions.SAVEPOINT_PREFIX, "savepoints");

// Upload options
config.set(S3StateBackendOptions.STATE_FILE_PART_SIZE, 10 * 1024 * 1024); // 10MB
```

## Usage

### Programmatic Configuration

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.backend.aws.s3.S3StateBackend;
import org.apache.flink.state.backend.aws.s3.config.S3StateBackendOptions;
import org.apache.flink.state.backend.aws.config.AWSConfigOptions;
import org.apache.flink.state.backend.aws.config.AWSConfigConstants.CredentialProvider;

// Create configuration
Configuration config = new Configuration();
config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");
config.set(S3StateBackendOptions.BUCKET_NAME, "flink-state");
config.set(S3StateBackendOptions.BASE_PATH, "my-job/state");
config.set(S3StateBackendOptions.STATE_FILE_COMPRESSION, true);

// Create state backend
S3StateBackend stateBackend = new S3StateBackend(config);

// Set state backend in Flink environment
env.setStateBackend(stateBackend);
```

### Configuration File

```yaml
# In flink-conf.yaml
state.backend: s3
state.backend.s3.bucket: flink-state
state.backend.s3.base-path: my-job/state
state.backend.s3.state-file-compression: true
aws.region: us-west-2
aws.credentials.provider: BASIC
aws.credentials.provider.basic.accesskeyid: your-access-key
aws.credentials.provider.basic.secretkey: your-secret-key
```

## S3 Object Structure

The S3 state backend uses object keys with the following structure:

```
<base-path>/<state-prefix>/<state-name>/<key-group>/<serialized-key-and-namespace><state-file-extension>
```

For example:
```
my-job/state/state/ValueState/42/serialized-key-and-namespace.state
```

This structure allows for efficient organization and retrieval of state data.

## Compression and Encryption

The S3 state backend supports compression and encryption of state data:

```java
// Enable compression (uses Snappy)
config.set(S3StateBackendOptions.STATE_FILE_COMPRESSION, true);

// Enable encryption (uses S3 server-side encryption)
config.set(S3StateBackendOptions.STATE_FILE_ENCRYPTION, true);
```

Compression can significantly reduce storage costs and improve transfer speeds, especially for large state sizes.

## Performance Considerations

- Use compression to reduce storage costs and network traffic
- Configure appropriate part sizes for multipart uploads
- Consider using S3 Transfer Acceleration for improved upload/download speeds
- Use appropriate S3 storage classes based on access patterns
- Monitor S3 metrics to identify bottlenecks

## Limitations

- Getting all keys for a specific namespace is not efficiently supported
- Priority queues are not yet supported
- Not recommended for production use yet
- Some advanced features like incremental state visitors are not yet implemented

## Dependencies

- flink-statebackend-aws-base
- AWS SDK for Java v2.x S3 module
- Snappy compression library
- SLF4J for logging

## License

This module is licensed under the Apache License 2.0.
