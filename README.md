# S3 Connector

The S3 connector enables Pipestream AI to crawl, sync, and index objects from S3-compatible storage systems. It provides a scalable, event-driven pipeline for processing large S3 buckets and integrating with downstream document processing services.

## Features

### 🔍 **Bucket Crawling**
- Complete S3 bucket crawling with pagination support
- Prefix filtering for targeted object discovery
- Event-driven crawling triggered by S3 notifications
- Individual object processing with full metadata extraction

### 🔐 **Security & Authentication**
- **Multiple credential sources**: Static credentials, KMS-resolved credentials, or anonymous access
- **KMS integration**: Secure credential management through external Key Management Services
- **Flexible configuration**: Support for different S3-compatible endpoints (AWS S3, MinIO, etc.)

### 📡 **gRPC API**
- `StartCrawl`: Initiate bucket crawling operations
- `TestBucketCrawl`: Validate connectivity and sample bucket contents
- Reactive implementation using Mutiny for high performance

### 📊 **Event Streaming**
- Kafka-based event publishing with Protobuf serialization
- Deterministic event ID generation for idempotent processing
- Integration with Apicurio Registry for schema management

### 🏗️ **Architecture**
- **Reactive**: Built with Mutiny for non-blocking operations
- **Scalable**: Client caching and connection pooling
- **Observable**: Comprehensive logging and monitoring
- **Testable**: Extensive test suite with MinIO integration

## Configuration

### KMS Integration

The connector supports resolving credentials from Key Management Services:

```properties
# Enable/disable KMS resolution
kms.enabled=true

# Development credentials (for testing)
kms.dev.access-key=minioadmin
kms.dev.secret-key=minioadmin
kms.dev.api-key=test-key
```

### S3 Connection Types

```protobuf
// Static credentials
S3ConnectionConfig {
  credentials_type: "static"
  access_key_id: "your-access-key"
  secret_access_key: "your-secret-key"
}

// KMS-resolved credentials
S3ConnectionConfig {
  credentials_type: "static"
  kms_access_key_ref: "kms://project/env/access-key"
  kms_secret_key_ref: "kms://project/env/secret-key"
}

// Anonymous access
S3ConnectionConfig {
  credentials_type: "anonymous"
}
```

## Development

### Prerequisites
- Java 21+
- Docker (for test containers)
- Gradle 8+

### Building
```bash
./gradlew build
```

### Testing
```bash
./gradlew test
```

### Running Locally
```bash
./gradlew quarkusDev
```

## Configuration

### Manual Configuration
The S3 connector currently requires manual configuration. Dynamic frontend forms are planned for a future release.

Configuration includes:
- S3 bucket name and optional prefix
- Authentication (static credentials, KMS references, or anonymous)
- AWS region and custom endpoints
- Crawl modes and performance settings

### Schema Registration (Deferred)
JSON Schema for dynamic frontend forms is available but registration is deferred to avoid protobuf dependency concerns. See the schema file at `src/main/resources/s3-connector-schema.json`.

## API Usage

### Start Bucket Crawl
```bash
grpcurl -plaintext localhost:8080 \
  ai.pipestream.connector.s3.v1.S3ConnectorControlService/StartCrawl \
  -d '{
    "datasource_id": "my-datasource",
    "bucket": "my-bucket",
    "connection_config": {
      "credentials_type": "static",
      "access_key_id": "key",
      "secret_access_key": "secret",
      "region": "us-east-1"
    }
  }'
```

### Test Bucket Connectivity
```bash
grpcurl -plaintext localhost:8080 \
  ai.pipestream.connector.s3.v1.S3ConnectorControlService/TestBucketCrawl \
  -d '{
    "bucket": "my-bucket",
    "connection_config": {
      "credentials_type": "anonymous"
    },
    "dry_run": true
  }'
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   gRPC API      │───▶│  Crawl Service   │───▶│  Kafka Events   │
│                 │    │                  │    │                 │
│ • StartCrawl    │    │ • Bucket listing │    │ • S3CrawlEvent  │
│ • TestBucketCrawl│   │ • Object metadata│    │ • Protobuf      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   S3 Client      │───▶│ Intake Service  │
                       │   Factory        │    │                 │
                       │                  │    │ • Document      │
                       │ • AWS SDK v2     │    │   processing    │
                       │ • Credential     │    └─────────────────┘
                       │   resolution     │
                       └──────────────────┘
```

## Contributing

This service follows standard Pipestream development practices:
- Comprehensive Javadoc documentation
- Reactive programming with Mutiny
- Extensive test coverage
- Container-based testing infrastructure
