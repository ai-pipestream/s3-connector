package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic test to verify MinIO setup and S3CrawlService configuration works.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource.class)
class S3CrawlServiceBasicTest {

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;
    
    // MinIO endpoint will be set via QuarkusTestResource properties
    @org.eclipse.microprofile.config.inject.ConfigProperty(name = "quarkus.s3.endpoint-override")
    String s3Endpoint;

    @Test
    @RunOnVertxContext
    void testMinioConnectionAndBasicCrawl(UniAsserter asserter) {
        String datasourceId = "test-datasource-1";
        String apiKey = "test-api-key";
        String bucket = "test-bucket"; // From MinioTestResource
        String testKey = "test-file.txt";
        String testContent = "Hello, S3!";

        // Create MinIO S3 connection config
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId("minioadmin")  // From MinioTestResource
            .setSecretAccessKey("minioadmin")  // From MinioTestResource
            .setRegion("us-east-1")
            .setEndpointOverride(s3Endpoint)  // From QuarkusTestResource properties
            .setPathStyleAccess(true)  // MinIO requires path-style access
            .build();

        // Register datasource config with MinIO connection details
        datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config);

        // Upload a test file to MinIO
        asserter.execute(() -> {
            assertNotNull(s3Endpoint, "MinIO endpoint should be set by QuarkusTestResource");

            AwsBasicCredentials credentials = AwsBasicCredentials.create(
                "minioadmin", // From MinioTestResource
                "minioadmin"  // From MinioTestResource
            );

            try (S3Client s3 = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .region(Region.of("us-east-1"))
                    .endpointOverride(URI.create(s3Endpoint))
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .build()) {
                
                s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(testKey)
                        .build(),
                        software.amazon.awssdk.core.sync.RequestBody.fromBytes(testContent.getBytes(StandardCharsets.UTF_8)));
            }
        });

        // Test that crawl service can list and process the object
        asserter.assertThat(() -> crawlService.crawlBucket(datasourceId, bucket, null),
            result -> {
                // If we get here without exception, the crawl completed successfully
                assertNull(result); // crawlBucket returns Uni<Void>, so result should be null
            });
    }
}
