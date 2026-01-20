package ai.pipestream.connector.s3;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.util.Map;

/**
 * Test resource for setting up MinIO container to simulate AWS S3 API.
 * <p>
 * This resource manages a MinIO container that provides an S3-compatible API
 * for testing. It creates a test bucket, configures Quarkus S3 extension properties
 * for MinIO access, and provides test data for integration tests.
 * </p>
 *
 * <h2>Container Configuration</h2>
 * <ul>
 *   <li>MinIO version: RELEASE.2025-01-20T14-49-07Z</li>
 *   <li>Default credentials: minioadmin/minioadmin</li>
 *   <li>Test bucket: test-bucket</li>
 *   <li>Path-style access enabled</li>
 * </ul>
 *
 * <h2>Test Data</h2>
 * <p>
 * Creates sample objects in the test bucket with various prefixes and content
 * types to support different test scenarios.
 * </p>
 *
 * @since 1.0.0
 */
public class MinioTestResource implements QuarkusTestResourceLifecycleManager {

    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET = "test-bucket";

    private GenericContainer<?> minio;

    @Override
    public Map<String, String> start() {
        minio = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-01-20T14-49-07Z"))
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                .withCommand("server", "/data", "--console-address", ":9001")
                .withExposedPorts(9000, 9001);
        minio.start();

        String endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);

        createBucket(endpoint);

        return Map.of(
                "quarkus.s3.endpoint-override", endpoint,
                "quarkus.s3.aws.region", "us-east-1",
                "quarkus.s3.aws.credentials.static-provider.access-key-id", ACCESS_KEY,
                "quarkus.s3.aws.credentials.static-provider.secret-access-key", SECRET_KEY,
                "quarkus.s3.path-style-access", "true"
        );
    }

    private static void createBucket(String endpoint) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);

        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.of("us-east-1"))
                .endpointOverride(URI.create(endpoint))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    /**
     * Get the MinIO endpoint URL.
     *
     * @return endpoint URL or null if container is not running
     */
    public String getEndpoint() {
        if (minio == null || !minio.isRunning()) {
            return null;
        }
        return "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);
    }

    /**
     * Get the default test bucket name.
     *
     * @return bucket name
     */
    public String getBucket() {
        return BUCKET;
    }

    /**
     * Get the MinIO access key.
     *
     * @return access key
     */
    public String getAccessKey() {
        return ACCESS_KEY;
    }

    /**
     * Get the MinIO secret key.
     *
     * @return secret key
     */
    public String getSecretKey() {
        return SECRET_KEY;
    }

    @Override
    public void stop() {
        if (minio != null) {
            minio.stop();
        }
    }
}