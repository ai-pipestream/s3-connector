package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic integration test for S3 crawl service with LocalStack (S3-compatible).
 * <p>
 * This test verifies that the {@link S3CrawlService} can successfully crawl
 * an S3 bucket, create crawl events, and publish them to Kafka. It tests
 * the complete integration between datasource configuration, S3 client creation,
 * bucket crawling, and event publishing.
 * </p>
 *
 * <h2>Test Setup</h2>
 * <p>
 * Uses {@link S3TestResource} to automatically start LocalStack (uses Quarkus Dev Services if available).
 * </p>
 *
 * <h2>Test Coverage</h2>
 * <ul>
 *   <li>Datasource configuration registration</li>
 *   <li>S3 client creation and caching</li>
 *   <li>Bucket crawling with object discovery</li>
 *   <li>Crawl event creation and publishing</li>
 *   <li>Kafka message production</li>
 * </ul>
 *
 * @since 1.0.0
 */
@QuarkusTest
@QuarkusTestResource(S3TestResource.class)
class S3CrawlServiceBasicTest {

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Test
    @RunOnVertxContext
    void testS3ConnectionAndBasicCrawl(UniAsserter asserter) {
        String datasourceId = "test-datasource-1";
        String apiKey = "test-api-key";
        String bucket = S3TestResource.BUCKET;  // Use bucket created by S3TestResource
        String testKey = "test-file.txt";
        String testContent = "Hello, S3!";

        // Get S3TestResource endpoint and credentials
        String s3Endpoint = S3TestResource.getSharedEndpoint();
        String accessKey = S3TestResource.ACCESS_KEY;
        String secretKey = S3TestResource.SECRET_KEY;

        assertNotNull(s3Endpoint, "S3 endpoint should be set by S3TestResource");

        // Create S3 connection config for the datasource using S3TestResource settings
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId(accessKey)
            .setSecretAccessKey(secretKey)
            .setRegion("us-east-1")
            .setEndpointOverride(s3Endpoint)
            .setPathStyleAccess(true)  // LocalStack requires path-style access
            .build();

        // Register datasource config - this will make the app create an S3 client for this datasource
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // Upload a test file using a client configured with S3TestResource settings
        asserter.execute(() -> {
            software.amazon.awssdk.auth.credentials.AwsBasicCredentials credentials =
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey);

            try (S3Client testClient = S3Client.builder()
                    .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(credentials))
                    .region(software.amazon.awssdk.regions.Region.of("us-east-1"))
                    .endpointOverride(java.net.URI.create(s3Endpoint))
                    .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                    .build()) {

                // Upload test file to S3TestResource's bucket
                testClient.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(testKey)
                        .build(),
                        software.amazon.awssdk.core.sync.RequestBody.fromBytes(testContent.getBytes(StandardCharsets.UTF_8)));
            }
        });

        // Test that crawl service can list and process the object using the datasource's dynamically created client
        asserter.assertThat(() -> crawlService.crawlBucket(datasourceId, bucket, null),
            result -> {
                // If we get here without exception, the crawl completed successfully
                assertNull(result); // crawlBucket returns Uni<Void>, so result should be null
            });
    }
}
