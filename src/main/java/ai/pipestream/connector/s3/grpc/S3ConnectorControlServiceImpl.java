package ai.pipestream.connector.s3.grpc;

import ai.pipestream.connector.s3.rest.S3ProtoJson;
import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.service.S3ClientFactory;
import ai.pipestream.connector.s3.service.S3TestCrawlService;
import ai.pipestream.connector.s3.v1.DeleteTestFileRequest;
import ai.pipestream.connector.s3.v1.DeleteTestFileResponse;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.S3FileLocation;
import ai.pipestream.connector.s3.v1.MutinyS3ConnectorControlServiceGrpc;
import ai.pipestream.connector.s3.v1.StartCrawlRequest;
import ai.pipestream.connector.s3.v1.StartCrawlResponse;
import ai.pipestream.connector.s3.v1.StreamFileLocationsRequest;
import ai.pipestream.connector.s3.v1.StreamFileLocationsResponse;
import ai.pipestream.connector.s3.v1.TestBucketCrawlRequest;
import ai.pipestream.connector.s3.v1.TestBucketCrawlResponse;
import ai.pipestream.connector.s3.v1.UploadTestFileRequest;
import ai.pipestream.connector.s3.v1.UploadTestFileResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URLConnection;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * gRPC service implementation for S3 connector control operations.
 * <p>
 * This service provides the control surface for unmanaged S3 connector crawls,
 * allowing external clients to trigger bucket crawling and testing operations.
 * It implements the {@link MutinyS3ConnectorControlServiceGrpc.S3ConnectorControlServiceImplBase}
 * using Mutiny for reactive programming.
 * </p>
 *
 * <h2>Authentication</h2>
 * <p>
 * All requests require authentication via the {@code x-api-key} header.
 * The API key is validated and used for datasource registration and authorization.
 * </p>
 *
 * <h2>Supported Operations</h2>
 * <ul>
 *   <li>{@link #startCrawl(StartCrawlRequest)} - Initiates a bucket crawl operation</li>
 *   <li>{@link #testBucketCrawl(TestBucketCrawlRequest)} - Tests connectivity and samples objects</li>
 * </ul>
 *
 * @since 1.0.0
 */
@GrpcService
public class S3ConnectorControlServiceImpl extends MutinyS3ConnectorControlServiceGrpc.S3ConnectorControlServiceImplBase {

    /**
     * Default constructor for CDI injection.
     */
    public S3ConnectorControlServiceImpl() {
    }

    private static final Logger LOG = Logger.getLogger(S3ConnectorControlServiceImpl.class);

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Inject
    S3TestCrawlService testCrawlService;

    @Inject
    S3ClientFactory clientFactory;

    /**
     * Initiates a crawl operation for an S3 bucket.
     * <p>
     * This method starts an asynchronous crawl of the specified S3 bucket, discovering
     * and processing all objects matching the optional prefix filter. The crawl operation
     * runs in the background and emits events for each discovered object.
     * </p>
     *
     * <h4>Request Validation</h4>
     * <ul>
     *   <li>{@code datasource_id} - Required (from header or request body)</li>
     *   <li>{@code x-api-key} - Required (from header)</li>
     *   <li>{@code connection_config} - Required</li>
     *   <li>{@code bucket} - Required</li>
     *   <li>{@code prefix} - Optional prefix filter</li>
     *   <li>{@code request_id} - Optional, auto-generated if not provided</li>
     * </ul>
     *
     * <h4>Side Effects</h4>
     * <ul>
     *   <li>Registers/updates datasource configuration</li>
     *   <li>Starts background crawl operation</li>
     *   <li>Publishes crawl events to Kafka</li>
     * </ul>
     *
     * @param request the {@link StartCrawlRequest} containing crawl parameters
     * @return a {@link Uni} that completes with {@link StartCrawlResponse} indicating
     *         whether the crawl was accepted, or fails with a gRPC status exception
     * @since 1.0.0
     */
    @Override
    public Uni<StartCrawlResponse> startCrawl(StartCrawlRequest request) {
        String headerApiKey = GrpcRequestContext.API_KEY.get();
        String headerDatasourceId = GrpcRequestContext.DATASOURCE_ID.get();

        String datasourceId = firstNonBlank(request.getDatasourceId(), headerDatasourceId);
        if (datasourceId == null) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("datasource_id is required")
                .asRuntimeException());
        }

        String apiKey = firstNonBlank(request.getApiKey(), headerApiKey);
        if (apiKey == null || apiKey.isBlank()) {
            return Uni.createFrom().failure(Status.UNAUTHENTICATED
                .withDescription("api_key is required (provide via x-api-key header or api_key field)")
                .asRuntimeException());
        }

        // Use connection config from request
        S3ConnectionConfig connectionConfig = request.getConnectionConfig();
        if (connectionConfig == null) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("connection_config is required")
                .asRuntimeException());
        }

        String bucket = firstNonBlank(request.getBucket());
        if (bucket == null) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("bucket is required")
                .asRuntimeException());
        }

        String prefix = firstNonBlank(request.getPrefix());
        String requestId = firstNonBlank(request.getRequestId(), UUID.randomUUID().toString());

        return datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, connectionConfig)
            .flatMap(v -> {
                LOG.infof("Received StartCrawl request: datasourceId=%s, bucket=%s, prefix=%s, requestId=%s",
                    datasourceId, bucket, prefix, requestId);

                return crawlService.crawlBucket(datasourceId, bucket, prefix);
            })
            .replaceWith(StartCrawlResponse.newBuilder()
                .setAccepted(true)
                .setMessage("Crawl accepted")
                .setRequestId(requestId)
                .setAcceptedAt(now())
                .build());
    }

    /**
     * Creates a protobuf Timestamp for the current instant.
     * <p>
     * Utility method for generating protobuf timestamp messages
     * representing the current time.
     * </p>
     *
     * @return a {@link Timestamp} protobuf message for the current time
     */
    private static Timestamp now() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();
    }

    /**
     * Tests S3 bucket connectivity and optionally samples objects.
     * <p>
     * This method validates S3 connection parameters and performs a test crawl
     * to verify access to the bucket. In dry-run mode, it counts objects without
     * emitting events. Otherwise, it returns a sample of discovered object keys.
     * </p>
     *
     * <h4>Request Validation</h4>
     * <ul>
     *   <li>{@code bucket} - Required</li>
     *   <li>{@code connection_config} - Required</li>
     *   <li>{@code prefix} - Optional prefix filter</li>
     *   <li>{@code dry_run} - Optional, defaults to false</li>
     *   <li>{@code max_sample} - Optional, defaults to 100</li>
     * </ul>
     *
     * <h4>Test Behavior</h4>
     * <ul>
     *   <li>Validates S3 credentials and bucket access</li>
     *   <li>Lists objects with optional prefix filtering</li>
     *   <li>In dry-run mode: returns total object count</li>
     *   <li>In normal mode: returns sample object keys</li>
     * </ul>
     *
     * @param request the {@link TestBucketCrawlRequest} containing test parameters
     * @return a {@link Uni} that completes with {@link TestBucketCrawlResponse} containing
     *         test results, or fails with a gRPC status exception
     * @since 1.0.0
     */
    @Override
    public Uni<TestBucketCrawlResponse> testBucketCrawl(TestBucketCrawlRequest request) {
        LOG.infof("Received TestBucketCrawl request: bucket=%s, prefix=%s, dryRun=%s, maxSample=%d",
            request.getBucket(), request.getPrefix(), request.getDryRun(), request.getMaxSample());

        // Validate request
        if (request.getBucket() == null || request.getBucket().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("bucket is required")
                .asRuntimeException());
        }

        if (request.getConnectionConfig() == null) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                .withDescription("connection_config is required")
                .asRuntimeException());
        }

        String bucket = request.getBucket();
        String prefix = firstNonBlank(request.getPrefix());
        boolean dryRun = request.getDryRun();
        int maxSample = request.getMaxSample() > 0 ? request.getMaxSample() : 100; // Default to 100 samples

        return testCrawlService.testBucketCrawl(
            request.getConnectionConfig(),
            bucket,
            prefix,
            dryRun,
            maxSample
        );
    }

    @Override
    public Multi<StreamFileLocationsResponse> streamFileLocations(StreamFileLocationsRequest request) {
        if (request.getBucket().isBlank()) {
            return Multi.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("bucket is required").asRuntimeException());
        }
        if (!request.hasConnectionConfig()) {
            return Multi.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("connection_config is required").asRuntimeException());
        }

        String bucket = request.getBucket();
        String prefix = request.getPrefix().isBlank() ? null : request.getPrefix();
        String extFilter = request.getExtensionFilter().isBlank() ? null : request.getExtensionFilter().toLowerCase();
        int maxFiles = request.getMaxFiles();

        LOG.infof("StreamFileLocations: bucket=%s, prefix=%s, extFilter=%s, maxFiles=%d",
                bucket, prefix, extFilter, maxFiles);

        AtomicInteger count = new AtomicInteger(0);

        return Multi.createFrom().uni(clientFactory.createTestClient(request.getConnectionConfig()))
                .flatMap(client -> {
                    // Paginate through all objects using recursive continuation tokens
                    return Multi.createFrom().emitter(emitter -> {
                        listPage(client, bucket, prefix, null, extFilter, maxFiles, count, emitter);
                    });
                });
    }

    private void listPage(software.amazon.awssdk.services.s3.S3AsyncClient client,
                           String bucket, String prefix, String continuationToken,
                           String extFilter, int maxFiles, AtomicInteger count,
                           io.smallrye.mutiny.subscription.MultiEmitter<? super StreamFileLocationsResponse> emitter) {

        ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                .bucket(bucket).maxKeys(1000);
        if (prefix != null) reqBuilder.prefix(prefix);
        if (continuationToken != null) reqBuilder.continuationToken(continuationToken);

        client.listObjectsV2(reqBuilder.build()).whenComplete((response, error) -> {
            if (error != null) {
                emitter.fail(error);
                try { client.close(); } catch (Exception ignored) {}
                return;
            }

            for (S3Object obj : response.contents()) {
                if (maxFiles > 0 && count.get() >= maxFiles) break;

                String key = obj.key();
                if (extFilter != null && !key.toLowerCase().endsWith(extFilter)) continue;

                String filename = key.contains("/") ? key.substring(key.lastIndexOf('/') + 1) : key;
                String contentType = URLConnection.guessContentTypeFromName(filename);
                if (contentType == null) contentType = "application/octet-stream";

                emitter.emit(StreamFileLocationsResponse.newBuilder()
                        .setFile(S3FileLocation.newBuilder()
                                .setKey(key)
                                .setSizeBytes(obj.size())
                                .setLastModified(Timestamp.newBuilder()
                                        .setSeconds(obj.lastModified().getEpochSecond())
                                        .setNanos(obj.lastModified().getNano()).build())
                                .setFilename(filename)
                                .setContentType(contentType)
                                .setSourceUrl("s3://" + bucket + "/" + key)
                                .build())
                        .build());
                count.incrementAndGet();
            }

            if (maxFiles > 0 && count.get() >= maxFiles) {
                emitter.complete();
                try { client.close(); } catch (Exception ignored) {}
                return;
            }

            String nextToken = response.nextContinuationToken();
            if (nextToken != null) {
                listPage(client, bucket, prefix, nextToken, extFilter, maxFiles, count, emitter);
            } else {
                emitter.complete();
                try { client.close(); } catch (Exception ignored) {}
            }
        });
    }

    /**
     * Uploads a test file to an S3 bucket.
     * <p>
     * This method is intended for use by E2E tests to seed data into a bucket
     * before triggering a crawl. It creates a temporary S3 client, uploads the
     * provided content, and returns the ETag and size on success.
     * </p>
     *
     * @param request the {@link UploadTestFileRequest} containing bucket, key, content, and connection config
     * @return a {@link Uni} that completes with {@link UploadTestFileResponse}
     * @since 1.0.0
     */
    @Override
    public Uni<UploadTestFileResponse> uploadTestFile(UploadTestFileRequest request) {
        if (request.getBucket().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("bucket is required").asRuntimeException());
        }
        if (request.getKey().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("key is required").asRuntimeException());
        }
        if (request.getContent().isEmpty()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("content is required").asRuntimeException());
        }
        if (!request.hasConnectionConfig()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("connection_config is required").asRuntimeException());
        }

        String bucket = request.getBucket();
        String key = request.getKey();
        byte[] content = request.getContent().toByteArray();
        String contentType = request.getContentType().isBlank() ? "application/octet-stream" : request.getContentType();

        LOG.infof("UploadTestFile: bucket=%s, key=%s, size=%d, contentType=%s", bucket, key, content.length, contentType);

        return Multi.createFrom().uni(clientFactory.createTestClient(request.getConnectionConfig()))
                .flatMap(client -> {
                    PutObjectRequest putRequest = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(contentType)
                            .contentLength((long) content.length)
                            .build();

                    return Multi.createFrom().completionStage(
                            client.putObject(putRequest, AsyncRequestBody.fromBytes(content))
                    ).onTermination().invoke(() -> {
                        try { client.close(); } catch (Exception ignored) {}
                    });
                })
                .toUni()
                .map(putResponse -> {
                    LOG.infof("UploadTestFile succeeded: bucket=%s, key=%s, etag=%s", bucket, key, putResponse.eTag());
                    return UploadTestFileResponse.newBuilder()
                            .setSuccess(true)
                            .setEtag(putResponse.eTag() != null ? putResponse.eTag() : "")
                            .setSizeBytes(content.length)
                            .build();
                })
                .onFailure().recoverWithItem(err -> {
                    LOG.warnf(err, "UploadTestFile failed: bucket=%s, key=%s", bucket, key);
                    return UploadTestFileResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage(err.getMessage() != null ? err.getMessage() : "Upload failed")
                            .build();
                });
    }

    /**
     * Deletes a test file from an S3 bucket.
     * <p>
     * This method is intended for use by E2E tests to clean up seeded test data
     * after a crawl has completed. It creates a temporary S3 client and deletes
     * the specified object.
     * </p>
     *
     * @param request the {@link DeleteTestFileRequest} containing bucket, key, and connection config
     * @return a {@link Uni} that completes with {@link DeleteTestFileResponse}
     * @since 1.0.0
     */
    @Override
    public Uni<DeleteTestFileResponse> deleteTestFile(DeleteTestFileRequest request) {
        if (request.getBucket().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("bucket is required").asRuntimeException());
        }
        if (request.getKey().isBlank()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("key is required").asRuntimeException());
        }
        if (!request.hasConnectionConfig()) {
            return Uni.createFrom().failure(Status.INVALID_ARGUMENT
                    .withDescription("connection_config is required").asRuntimeException());
        }

        String bucket = request.getBucket();
        String key = request.getKey();

        LOG.infof("DeleteTestFile: bucket=%s, key=%s", bucket, key);

        return Multi.createFrom().uni(clientFactory.createTestClient(request.getConnectionConfig()))
                .flatMap(client -> {
                    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build();

                    return Multi.createFrom().completionStage(
                            client.deleteObject(deleteRequest)
                    ).onTermination().invoke(() -> {
                        try { client.close(); } catch (Exception ignored) {}
                    });
                })
                .toUni()
                .map(deleteResponse -> {
                    LOG.infof("DeleteTestFile succeeded: bucket=%s, key=%s", bucket, key);
                    return DeleteTestFileResponse.newBuilder()
                            .setSuccess(true)
                            .build();
                })
                .onFailure().recoverWithItem(err -> {
                    LOG.warnf(err, "DeleteTestFile failed: bucket=%s, key=%s", bucket, key);
                    return DeleteTestFileResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage(err.getMessage() != null ? err.getMessage() : "Delete failed")
                            .build();
                });
    }

    /**
     * Returns the first non-blank string from the provided values.
     * <p>
     * Utility method for selecting the first non-null, non-blank string
     * from a variable number of arguments. Useful for implementing
     * header/request body precedence logic.
     * </p>
     *
     * @param values the string values to check, in order of preference
     * @return the first non-blank string, or null if all values are blank or null
     */
    private static String firstNonBlank(String... values) {
        return S3ProtoJson.firstNonBlank(values);
    }
}
