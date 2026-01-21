package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.common.QuarkusTestResource;

/**
 * Integration test for crawling public S3 buckets (e.g., NOAA GSOD).
 * 
 * This test crawls the public NOAA Global Surface Summary of the Day bucket
 * equivalent to: aws s3 ls "s3://noaa-gsod-pds" --recursive --no-sign-request
 * 
 * Uses AwsS3AnonymousTestResource to configure anonymous S3 access and
 * S3AnonymousClientProducer to create an S3AsyncClient with anonymous credentials.
 */
@QuarkusTest
@QuarkusTestResource(AwsS3AnonymousTestResource.class)
class S3CrawlServiceIntegrationTest {

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;

    /**
     * Test crawling the public NOAA GSOD bucket.
     * Uses a small prefix to limit results and keep the test reasonable.
     */
    @Test
    @RunOnVertxContext
    void testNoaaGsodBucketCrawl(UniAsserter asserter) {
        String datasourceId = "test-noaa-gsod";
        String apiKey = "test-api-key";
        String bucket = "noaa-gsod-pds";
        
        // Use a small prefix to limit results (e.g., 2024 data)
        // This is equivalent to: aws s3 ls "s3://noaa-gsod-pds/2024/" --recursive --no-sign-request
        String prefix = "2024/";

        // Register datasource config with anonymous S3 config for public bucket
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("anonymous")
            .setRegion("us-east-1")
            .build();

        // Register datasource configuration (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // Test that crawl service can list and process objects from the public bucket
        asserter.assertThat(() -> crawlService.crawlBucket(datasourceId, bucket, prefix),
            result -> {
                // If we get here without exception, the crawl completed successfully
                assertNull(result); // crawlBucket returns Uni<Void>, so result should be null
            });
    }

    /**
     * Test crawling the public NOAA GSOD bucket root (limited to small number of objects).
     * This tests listing at the bucket root level.
     */
    @Test
    @RunOnVertxContext
    void testNoaaGsodBucketRootCrawl(UniAsserter asserter) {
        String datasourceId = "test-noaa-gsod-root";
        String apiKey = "test-api-key";
        String bucket = "noaa-gsod-pds";
        
        // Crawl bucket root (no prefix) - this will list directories and top-level objects
        // Note: This will only process the first page of results due to pagination,
        // but verifies the basic listing works
        String prefix = null;

        // Register datasource config with anonymous S3 config for public bucket
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("anonymous")
            .setRegion("us-east-1")
            .build();

        // Register datasource configuration (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // Test that crawl service can list objects at the root level
        asserter.assertThat(() -> crawlService.crawlBucket(datasourceId, bucket, prefix),
            result -> {
                // If we get here without exception, the crawl completed successfully
                assertNull(result);
            });
    }
}
