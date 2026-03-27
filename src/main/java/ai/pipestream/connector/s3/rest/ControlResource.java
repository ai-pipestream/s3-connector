package ai.pipestream.connector.s3.rest;

import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.S3TestCrawlService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.TestBucketCrawlResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * REST control plane mirroring {@link ai.pipestream.connector.s3.grpc.S3ConnectorControlServiceImpl}.
 * Provides endpoints for starting S3 crawls and testing bucket connectivity.
 */
@Path("/api/control")
@Produces(MediaType.APPLICATION_JSON)
public class ControlResource {

    /**
     * Default constructor for ControlResource.
     */
    public ControlResource() {
    }

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Inject
    S3CrawlService crawlService;

    @Inject
    S3TestCrawlService testCrawlService;

    @Inject
    ObjectMapper objectMapper;

    /**
     * Starts a crawl for the specified bucket and prefix.
     *
     * @param headerApiKey       API key from headers
     * @param headerDatasourceId datasource ID from headers
     * @param body               request body containing crawl details
     * @return response indicating if the crawl was accepted
     */
    @POST
    @Path("start-crawl")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<StartCrawlResponseDto> startCrawl(
        @HeaderParam("x-api-key") String headerApiKey,
        @HeaderParam("x-datasource-id") String headerDatasourceId,
        StartCrawlRequestJson body
    ) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        String datasourceId = S3ProtoJson.firstNonBlank(body.datasourceId, headerDatasourceId);
        if (datasourceId == null) {
            throw new IllegalArgumentException("datasource_id is required");
        }
        if (headerApiKey == null || headerApiKey.isBlank()) {
            throw new IllegalArgumentException("x-api-key header is required");
        }

        final S3ConnectionConfig connectionConfig;
        try {
            connectionConfig = S3ProtoJson.parseConnectionConfig(objectMapper, body.connectionConfig);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid connection_config: " + e.getMessage(), e);
        }

        String bucket = S3ProtoJson.firstNonBlank(body.bucket);
        if (bucket == null) {
            throw new IllegalArgumentException("bucket is required");
        }
        String prefix = S3ProtoJson.firstNonBlank(body.prefix);
        String requestId = S3ProtoJson.firstNonBlank(body.requestId, UUID.randomUUID().toString());

        return datasourceConfigService.registerDatasourceConfig(datasourceId, headerApiKey, connectionConfig)
            .flatMap(v -> crawlService.crawlBucket(datasourceId, bucket, prefix))
            .replaceWith(new StartCrawlResponseDto(true, "Crawl accepted", requestId, Instant.now()));
    }

    /**
     * Tests connectivity and access to a specific S3 bucket.
     *
     * @param body request body containing connection details and bucket to test
     * @return response with test results
     */
    @POST
    @Path("test-bucket")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<TestBucketCrawlResponseDto> testBucket(TestBucketRequestJson body) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        if (body.bucket == null || body.bucket.isBlank()) {
            throw new IllegalArgumentException("bucket is required");
        }
        final S3ConnectionConfig connectionConfig;
        try {
            connectionConfig = S3ProtoJson.parseConnectionConfig(objectMapper, body.connectionConfig);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid connection_config: " + e.getMessage(), e);
        }
        String prefix = S3ProtoJson.firstNonBlank(body.prefix);
        boolean dryRun = body.dryRun;
        int maxSample = body.maxSample > 0 ? body.maxSample : 100;

        return testCrawlService.testBucketCrawl(connectionConfig, body.bucket, prefix, dryRun, maxSample)
            .map(ControlResource::toTestDto);
    }

    private static TestBucketCrawlResponseDto toTestDto(TestBucketCrawlResponse r) {
        Instant testedAt = r.hasTestedAt()
            ? Instant.ofEpochSecond(r.getTestedAt().getSeconds(), r.getTestedAt().getNanos())
            : null;
        List<String> keys = r.getSampleObjectKeysList();
        return new TestBucketCrawlResponseDto(
            r.getSuccess(),
            r.getTotalObjects(),
            keys,
            r.getErrorMessage().isBlank() ? null : r.getErrorMessage(),
            testedAt
        );
    }

    /**
     * Request DTO for starting a crawl.
     */
    public static class StartCrawlRequestJson {
        /**
         * Datasource identifier.
         */
        public String datasourceId;
        /**
         * S3 connection configuration in JSON format.
         */
        public JsonNode connectionConfig;
        /**
         * S3 bucket to crawl.
         */
        public String bucket;
        /**
         * Optional S3 prefix to filter objects.
         */
        public String prefix;
        /**
         * Optional request ID for tracking.
         */
        public String requestId;

        /**
         * Default constructor for StartCrawlRequestJson.
         */
        public StartCrawlRequestJson() {
        }
    }

    /**
     * Request DTO for testing a bucket.
     */
    public static class TestBucketRequestJson {
        /**
         * S3 connection configuration in JSON format.
         */
        public JsonNode connectionConfig;
        /**
         * S3 bucket to test.
         */
        public String bucket;
        /**
         * Optional S3 prefix.
         */
        public String prefix;
        /**
         * Whether to perform a dry run (no actual crawling).
         */
        public boolean dryRun;
        /**
         * Maximum number of sample keys to return.
         */
        public int maxSample;

        /**
         * Default constructor for TestBucketRequestJson.
         */
        public TestBucketRequestJson() {
        }
    }

    /**
     * Response DTO for a crawl start request.
     *
     * @param accepted   whether the request was accepted
     * @param message    status message
     * @param requestId  request ID
     * @param acceptedAt timestamp of acceptance
     */
    public record StartCrawlResponseDto(boolean accepted, String message, String requestId, Instant acceptedAt) {
    }

    /**
     * Response DTO for a bucket test request.
     *
     * @param success           whether the test was successful
     * @param totalObjects      estimated total objects in the bucket
     * @param sampleObjectKeys  sample list of keys found
     * @param errorMessage      error message if success is false
     * @param testedAt          timestamp of the test
     */
    public record TestBucketCrawlResponseDto(
        boolean success,
        long totalObjects,
        List<String> sampleObjectKeys,
        String errorMessage,
        Instant testedAt
    ) {
    }
}

