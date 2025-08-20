# Flink DynamoDB State Backend

This module provides a state backend implementation for Apache Flink that uses Amazon DynamoDB for state storage. It enables Flink applications to store their state in a fully managed, highly available, and scalable NoSQL database service.

## Overview

The DynamoDB state backend:

- Stores keyed state in DynamoDB tables
- Automatically creates and manages DynamoDB tables
- Supports all standard Flink keyed state types
- Provides configurable throughput settings
- Includes local DynamoDB support for testing

## Key Components

### DynamoDbStateBackend

The `DynamoDbStateBackend` class is the main entry point for using DynamoDB as a state backend. It:

- Creates and configures DynamoDB clients
- Manages DynamoDB table creation and validation
- Creates keyed state backends for operators
- Handles configuration and customization

### DynamoDbKeyedStateBackend

The `DynamoDbKeyedStateBackend` class implements the keyed state backend interface for DynamoDB. It:

- Stores and retrieves state data in DynamoDB
- Manages serialization and deserialization of state
- Implements state iteration and querying
- Handles checkpoints and savepoints

### DynamoDbKeyedStateBackendBuilder

The `DynamoDbKeyedStateBackendBuilder` class provides a builder pattern for creating DynamoDB keyed state backends. It:

- Configures the keyed state backend with appropriate settings
- Sets up snapshot strategies for checkpoints
- Handles state restoration from previous checkpoints

### DynamoDbStateBackendFactory

The `DynamoDbStateBackendFactory` class implements the Flink `StateBackendFactory` interface for the DynamoDB state backend. It:

- Creates DynamoDB state backends from configuration
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

// DynamoDB table configuration
config.set(DynamoDbStateBackendOptions.TABLE_NAME, "flink-state");
```

### Optional Configuration

```java
// Use on-demand capacity mode (instead of provisioned)
config.set(DynamoDbStateBackendOptions.USE_ON_DEMAND, true);

// Configure provisioned capacity (when not using on-demand)
config.set(DynamoDbStateBackendOptions.READ_CAPACITY_UNITS, 20);
config.set(DynamoDbStateBackendOptions.WRITE_CAPACITY_UNITS, 20);

// Local DynamoDB for testing
config.set(DynamoDbStateBackendOptions.LOCAL_DB_PATH, "/path/to/local/dynamodb");
config.set(DynamoDbStateBackendOptions.LOCAL_IN_MEMORY, true);
```

## Usage

### Programmatic Configuration

```java
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.backend.aws.dynamodb.DynamoDbStateBackend;
import org.apache.flink.state.backend.aws.dynamodb.config.DynamoDbStateBackendOptions;
import org.apache.flink.state.backend.aws.config.AWSConfigOptions;
import org.apache.flink.state.backend.aws.config.AWSConfigConstants.CredentialProvider;

// Create configuration
Configuration config = new Configuration();
config.set(AWSConfigOptions.AWS_REGION_OPTION, "us-west-2");
config.set(AWSConfigOptions.AWS_CREDENTIALS_PROVIDER_OPTION, CredentialProvider.BASIC);
config.set(AWSConfigOptions.AWS_ACCESS_KEY_ID_OPTION, "your-access-key");
config.set(AWSConfigOptions.AWS_SECRET_ACCESS_KEY_OPTION, "your-secret-key");
config.set(DynamoDbStateBackendOptions.TABLE_NAME, "flink-state");
config.set(DynamoDbStateBackendOptions.USE_ON_DEMAND, true);

// Create state backend
DynamoDbStateBackend stateBackend = new DynamoDbStateBackend(config);

// Set state backend in Flink environment
env.setStateBackend(stateBackend);
```

### Configuration File

```yaml
# In flink-conf.yaml
state.backend: dynamodb
state.backend.dynamodb.table: flink-state
state.backend.dynamodb.use-on-demand: true
aws.region: us-west-2
aws.credentials.provider: BASIC
aws.credentials.provider.basic.accesskeyid: your-access-key
aws.credentials.provider.basic.secretkey: your-secret-key
```

## DynamoDB Table Structure

The DynamoDB state backend uses a table with the following structure:

- **Partition Key**: A combination of operator ID and state name
- **Sort Key**: A serialized combination of key and namespace
- **Value**: The serialized state value

This structure allows for efficient querying of state by key and namespace while maintaining good distribution of data across DynamoDB partitions.

## Local Testing

For testing purposes, the DynamoDB state backend supports using a local DynamoDB instance:

```java
// Configure local DynamoDB
config.set(DynamoDbStateBackendOptions.LOCAL_DB_PATH, "/path/to/local/dynamodb");
config.set(DynamoDbStateBackendOptions.LOCAL_IN_MEMORY, true);
```

This allows you to test your Flink application without connecting to the actual AWS DynamoDB service.

## Performance Considerations

- Use on-demand capacity mode for variable workloads
- Configure appropriate provisioned capacity for predictable workloads
- Consider using local caching for frequently accessed state
- Monitor DynamoDB metrics to identify bottlenecks
- Use appropriate TTL settings for state expiration

## Limitations

- Currently does not support operator state
- Not recommended for production use yet
- Performance characteristics may differ from other state backends

## Dependencies

- flink-statebackend-aws-base
- AWS SDK for Java v2.x DynamoDB module
- SLF4J for logging

## License

This module is licensed under the Apache License 2.0.
