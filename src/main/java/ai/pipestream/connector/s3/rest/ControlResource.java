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
 */
@Path("/api/control")
@Produces(MediaType.APPLICATION_JSON)
public class ControlResource {

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Inject
    S3CrawlService crawlService;

    @Inject
    S3TestCrawlService testCrawlService;

    @Inject
    ObjectMapper objectMapper;

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

    public static class StartCrawlRequestJson {
        public String datasourceId;
        public JsonNode connectionConfig;
        public String bucket;
        public String prefix;
        public String requestId;
    }

    public static class TestBucketRequestJson {
        public JsonNode connectionConfig;
        public String bucket;
        public String prefix;
        public boolean dryRun;
        public int maxSample;
    }

    public record StartCrawlResponseDto(boolean accepted, String message, String requestId, Instant acceptedAt) {
    }

    public record TestBucketCrawlResponseDto(
        boolean success,
        long totalObjects,
        List<String> sampleObjectKeys,
        String errorMessage,
        Instant testedAt
    ) {
    }
}
