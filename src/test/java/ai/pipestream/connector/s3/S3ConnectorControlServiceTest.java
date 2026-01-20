package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.S3TestCrawlService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.TestBucketCrawlRequest;
import ai.pipestream.connector.s3.v1.TestBucketCrawlResponse;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for S3 connector control service functionality.
 * <p>
 * This test class verifies the behavior of the {@link ai.pipestream.connector.s3.grpc.S3ConnectorControlServiceImpl}
 * and related services, focusing on gRPC API endpoints and S3 connectivity testing.
 * </p>
 *
 * <h2>Test Coverage</h2>
 * <ul>
 *   <li>S3 bucket connectivity testing with anonymous credentials</li>
 *   <li>Error handling for invalid configurations</li>
 *   <li>Integration with {@link S3TestCrawlService}</li>
 * </ul>
 *
 * @since 1.0.0
 */
@QuarkusTest
class S3ConnectorControlServiceTest {

    @Inject
    S3TestCrawlService testCrawlService;

    @Test
    void testTestBucketCrawlAnonymous() {
        // Create anonymous S3 config
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("anonymous")
            .setRegion("us-east-1")
            .build();

        // Call the service directly with a non-existent bucket to test error handling
        TestBucketCrawlResponse response = testCrawlService.testBucketCrawl(
            s3Config,
            "definitely-non-existent-bucket-12345", // Use a bucket that definitely doesn't exist
            null, // no prefix
            true, // dry run
            10    // max sample
        ).subscribe().withSubscriber(UniAssertSubscriber.create())
         .awaitItem(java.time.Duration.ofSeconds(10)) // Timeout for network calls
         .getItem();

        // Verify response structure
        assertNotNull(response);
        // Since we're using a non-existent bucket, we expect failure but with proper error message
        assertFalse(response.getSuccess(), "Should fail for non-existent bucket");
        assertNotNull(response.getErrorMessage(), "Error message should be provided");
        assertFalse(response.getErrorMessage().isBlank(), "Error message should not be blank");
    }
}