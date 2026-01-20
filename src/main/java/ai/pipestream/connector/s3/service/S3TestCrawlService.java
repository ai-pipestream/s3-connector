package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.TestBucketCrawlResponse;
import com.google.protobuf.Timestamp;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for testing S3 bucket connectivity and performing dry-run crawls.
 * <p>
 * This service validates S3 connection configurations and provides testing capabilities
 * for the {@code TestBucketCrawl} gRPC method. It can perform connectivity tests and
 * optionally count all objects in a bucket without emitting crawl events to Kafka.
 * </p>
 *
 * <h2>Test Modes</h2>
 * <ul>
 *   <li><strong>Connectivity Test</strong>: Validates S3 credentials and bucket access</li>
 *   <li><strong>Dry Run</strong>: Counts all objects in the bucket with optional prefix filtering</li>
 *   <li><strong>Sample Collection</strong>: Returns sample object keys for verification</li>
 * </ul>
 *
 * <h2>Resource Management</h2>
 * <p>
 * The service creates temporary S3 clients for testing that are properly closed
 * after use, ensuring no resource leaks during testing operations.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3TestCrawlService {

    /**
     * Default constructor for CDI injection.
     */
    public S3TestCrawlService() {
    }

    private static final Logger LOG = Logger.getLogger(S3TestCrawlService.class);

    @Inject
    S3ClientFactory clientFactory;

    /**
     * Tests S3 bucket connectivity and optionally performs a dry-run crawl.
     * <p>
     * This method validates the S3 connection configuration by attempting to
     * access the specified bucket. In dry-run mode, it counts all objects matching
     * the optional prefix filter. Otherwise, it returns a sample of object keys
     * from the first page of results.
     * </p>
     *
     * @param config the S3 connection configuration to test
     * @param bucket the name of the S3 bucket to test access to
     * @param prefix optional prefix filter for objects (may be null)
     * @param dryRun if true, counts all objects; if false, returns sample keys
     * @param maxSample maximum number of sample object keys to collect (0 for all)
     * @return a {@link Uni} that completes with the test results in a {@link TestBucketCrawlResponse}
     * @since 1.0.0
     */
    public Uni<TestBucketCrawlResponse> testBucketCrawl(
            S3ConnectionConfig config,
            String bucket,
            String prefix,
            boolean dryRun,
            int maxSample) {

        LOG.infof("Testing S3 bucket connectivity: bucket=%s, prefix=%s, dryRun=%s, maxSample=%d",
            bucket, prefix, dryRun, maxSample);

        // Create a temporary test client (not cached)
        return clientFactory.createTestClient(config)
            .flatMap(testClient ->
                performTest(testClient, bucket, prefix, dryRun, maxSample)
                    .onFailure().invoke(error -> {
                        LOG.errorf(error, "S3 test failed: bucket=%s, prefix=%s", bucket, prefix);
                        // Close the test client on failure
                        try {
                            testClient.close();
                        } catch (Exception e) {
                            LOG.warnf(e, "Error closing test S3 client");
                        }
                    })
                    .onTermination().invoke(() -> {
                        // Always close the test client
                        try {
                            testClient.close();
                            LOG.debug("Closed test S3 client");
                        } catch (Exception e) {
                            LOG.warnf(e, "Error closing test S3 client");
                        }
                    })
            )
            .onFailure().recoverWithItem(error ->
                createErrorResponse("Failed to create S3 client: " + error.getMessage())
            );
    }

    private Uni<TestBucketCrawlResponse> performTest(
            S3AsyncClient client,
            String bucket,
            String prefix,
            boolean dryRun,
            int maxSample) {

        // First, try to list objects to test connectivity
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucket)
            .maxKeys(Math.min(maxSample > 0 ? maxSample : 100, 1000)); // Limit for test

        if (prefix != null && !prefix.isBlank()) {
            requestBuilder.prefix(prefix);
        }

        ListObjectsV2Request request = requestBuilder.build();

        return Uni.createFrom().completionStage(client.listObjectsV2(request))
            .flatMap(response -> {
                // Connectivity successful, now handle the response
                List<S3Object> contents = response.contents();

                if (dryRun) {
                    // If dry run, we need to count all objects
                    return countAllObjects(client, bucket, prefix, response, maxSample);
                } else {
                    // Not dry run, just return sample from first page
                    return Uni.createFrom().item(createSuccessResponse(
                        contents.size(), // This is just the first page count
                        extractSampleKeys(contents, maxSample),
                        "Connectivity test successful"
                    ));
                }
            })
            .onFailure().recoverWithItem(error ->
                createErrorResponse("S3 connectivity test failed: " + error.getMessage())
            );
    }

    private Uni<TestBucketCrawlResponse> countAllObjects(
            S3AsyncClient client,
            String bucket,
            String prefix,
            ListObjectsV2Response firstResponse,
            int maxSample) {

        AtomicLong totalCount = new AtomicLong(firstResponse.contents().size());
        List<String> sampleKeys = new ArrayList<>(extractSampleKeys(firstResponse.contents(), maxSample));

        // If there are more pages or we want to count everything, continue
        return countRemainingPages(client, bucket, prefix, firstResponse, totalCount, sampleKeys, maxSample)
            .map(ignored -> createSuccessResponse(
                totalCount.get(),
                sampleKeys,
                "Dry run completed successfully"
            ));
    }

    private Uni<Void> countRemainingPages(
            S3AsyncClient client,
            String bucket,
            String prefix,
            ListObjectsV2Response response,
            AtomicLong totalCount,
            List<String> sampleKeys,
            int maxSample) {

        String nextToken = response.nextContinuationToken();
        if (nextToken == null) {
            return Uni.createFrom().voidItem();
        }

        ListObjectsV2Request nextRequest = ListObjectsV2Request.builder()
            .bucket(bucket)
            .continuationToken(nextToken)
            .maxKeys(Math.min(maxSample > 0 ? maxSample : 100, 1000))
            .build();

        if (prefix != null && !prefix.isBlank()) {
            nextRequest = nextRequest.toBuilder().prefix(prefix).build();
        }

        return Uni.createFrom().completionStage(client.listObjectsV2(nextRequest))
            .flatMap(nextResponse -> {
                List<S3Object> contents = nextResponse.contents();
                totalCount.addAndGet(contents.size());

                // Add more sample keys if we haven't reached the limit
                if (sampleKeys.size() < maxSample || maxSample <= 0) {
                    List<String> newKeys = extractSampleKeys(contents,
                        maxSample > 0 ? maxSample - sampleKeys.size() : contents.size());
                    sampleKeys.addAll(newKeys);
                }

                // Continue with next page if needed
                return countRemainingPages(client, bucket, prefix, nextResponse, totalCount, sampleKeys, maxSample);
            });
    }

    private List<String> extractSampleKeys(List<S3Object> objects, int maxSample) {
        return objects.stream()
            .limit(maxSample > 0 ? maxSample : objects.size())
            .map(S3Object::key)
            .toList();
    }

    private TestBucketCrawlResponse createSuccessResponse(long totalObjects, List<String> sampleKeys, String message) {
        return TestBucketCrawlResponse.newBuilder()
            .setSuccess(true)
            .setTotalObjects(totalObjects)
            .addAllSampleObjectKeys(sampleKeys)
            .setTestedAt(now())
            .build();
    }

    private TestBucketCrawlResponse createErrorResponse(String errorMessage) {
        return TestBucketCrawlResponse.newBuilder()
            .setSuccess(false)
            .setErrorMessage(errorMessage)
            .setTestedAt(now())
            .build();
    }

    private static Timestamp now() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();
    }
}