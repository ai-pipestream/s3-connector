package ai.pipestream.connector.s3;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;

/**
 * Test resource for configuring S3 client for anonymous access to public buckets (e.g., NOAA GSOD).
 * 
 * This configures the S3 client to use anonymous credentials, which allows access to
 * public S3 buckets without authentication.
 * 
 * Equivalent to: aws s3 ls "s3://noaa-gsod-pds" --recursive --no-sign-request
 */
public class AwsS3AnonymousTestResource implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        // Configure S3 for anonymous access to public AWS buckets
        // Override all MinIO/static credential settings
        return Map.of(
                // Clear endpoint override (use real AWS S3, not MinIO)
                // Note: Using null or not setting it should work, but empty string might be needed
                "quarkus.s3.endpoint-override", "",
                "quarkus.s3.aws.region", "us-east-1",
                // Use anonymous credentials for public bucket access
                "quarkus.s3.aws.credentials.type", "anonymous",
                // Disable path-style access for real AWS S3 (use virtual-hosted style)
                "quarkus.s3.path-style-access", "false"
        );
    }

    @Override
    public void stop() {
        // No cleanup needed
    }
}
