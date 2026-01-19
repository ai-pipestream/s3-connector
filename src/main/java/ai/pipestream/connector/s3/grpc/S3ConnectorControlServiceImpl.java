package ai.pipestream.connector.s3.grpc;

import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.MutinyS3ConnectorControlServiceGrpc;
import ai.pipestream.connector.s3.v1.StartCrawlRequest;
import ai.pipestream.connector.s3.v1.StartCrawlResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

/**
 * gRPC control surface for unmanaged S3 connector crawls.
 */
@GrpcService
public class S3ConnectorControlServiceImpl extends MutinyS3ConnectorControlServiceGrpc.S3ConnectorControlServiceImplBase {

    private static final Logger LOG = Logger.getLogger(S3ConnectorControlServiceImpl.class);

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;

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

        if (headerApiKey == null || headerApiKey.isBlank()) {
            return Uni.createFrom().failure(Status.UNAUTHENTICATED
                .withDescription("x-api-key header is required")
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

        datasourceConfigService.registerDatasourceConfig(datasourceId, headerApiKey);

        LOG.infof("Received StartCrawl request: datasourceId=%s, bucket=%s, prefix=%s, requestId=%s",
            datasourceId, bucket, prefix, requestId);

        return crawlService.crawlBucket(datasourceId, bucket, prefix)
            .replaceWith(StartCrawlResponse.newBuilder()
                .setAccepted(true)
                .setMessage("Crawl accepted")
                .setRequestId(requestId)
                .setAcceptedAt(now())
                .build());
    }

    private static Timestamp now() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
