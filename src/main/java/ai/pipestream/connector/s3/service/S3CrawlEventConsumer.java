package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.client.ConnectorIntakeClient;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Consumes S3 crawl events from Kafka, downloads the referenced objects from S3,
 * and streams them to the connector-intake-service for processing.
 */
@ApplicationScoped
public class S3CrawlEventConsumer {
    private static final String DEBUG_SESSION_ID = "a0041d";
    private static final String DEBUG_RUN_ID = "pre-fix";

    /** Creates a new S3CrawlEventConsumer (CDI managed). */
    public S3CrawlEventConsumer() {
    }

    private static final Logger LOG = Logger.getLogger(S3CrawlEventConsumer.class);

    @Inject
    S3ClientFactory clientFactory;

    @Inject
    ConnectorIntakeClient intakeClient;

    @Inject
    DatasourceConfigService datasourceConfigService;

    /**
     * Processes an incoming S3 crawl event by downloading the object and uploading it to intake.
     *
     * @param event the S3 crawl event describing which object to process
     * @return a Uni that completes when the object has been uploaded
     */
    @Incoming("s3-crawl-events-in")
    public Uni<Void> processCrawlEvent(S3CrawlEvent event) {
        long startMs = System.currentTimeMillis();
        String datasourceId = event != null ? event.getDatasourceId() : "unknown";
        String sourceUrl = event != null ? event.getSourceUrl() : "unknown";
        String bucket = event != null ? event.getBucket() : "unknown";
        String key = event != null ? event.getKey() : "unknown";

        // #region agent log
        logDebug("A", "S3CrawlEventConsumer#processCrawlEvent", "received event", datasourceId, sourceUrl, bucket, key, startMs, -1, "start");
        // #endregion

        LOG.infof("Processing S3 crawl event: datasourceId=%s, sourceUrl=%s", 
            event.getDatasourceId(), event.getSourceUrl());

        return datasourceConfigService.getDatasourceConfig(event.getDatasourceId())
            .emitOn(Infrastructure.getDefaultWorkerPool())
            .flatMap(config -> clientFactory.getOrCreateClient(config.datasourceId(), config.s3Config())
                .flatMap(client -> {
                    // #region agent log
                    logDebug("A", "S3CrawlEventConsumer#downloadObject", "client ready", datasourceId, sourceUrl, bucket, key, startMs, -1, "client-ready");
                    // #endregion
                    return Uni.createFrom().completionStage(downloadObject(client, event))
                        .onItem().invoke(response -> logDebug("A", "S3CrawlEventConsumer#downloadObject", "received headers", datasourceId, sourceUrl, bucket, key, startMs, response.response().contentLength(), "downloaded"))
                        .flatMap(s3Response -> {
                            LOG.infof("DEBUG: Received headers, starting stream for %s", event.getSourceUrl());
                            return intakeClient.uploadRaw(
                                event.getDatasourceId(),
                                config.apiKey(),
                                event.getSourceUrl(),
                                event.getBucket(),
                                event.getKey(),
                                s3Response.response().contentType(),
                                s3Response.response().contentLength(),
                                s3Response
                            );
                        })
                        .onItem().invoke(() -> logDebug("B", "S3CrawlEventConsumer#processCrawlEvent", "upload completed", datasourceId, sourceUrl, bucket, key, startMs, 0, "success"))
                        .onFailure().invoke(error -> {
                            logDebug("C", "S3CrawlEventConsumer#processCrawlEvent", "processing failed", datasourceId, sourceUrl, bucket, key, startMs, 0, errorClass(error));
                            LOG.errorf(error, "Failed to process crawl event: datasourceId=%s, sourceUrl=%s",
                                event.getDatasourceId(), event.getSourceUrl());
                        })
                        .replaceWithVoid();
                })
            );
    }

    private java.util.concurrent.CompletionStage<ResponseInputStream<GetObjectResponse>> downloadObject(S3AsyncClient client, S3CrawlEvent event) {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
            .bucket(event.getBucket())
            .key(event.getKey());

        if (event.getVersionId() != null && !event.getVersionId().isEmpty()) {
            requestBuilder.versionId(event.getVersionId());
        }

        GetObjectRequest request = requestBuilder.build();
        return client.getObject(request, AsyncResponseTransformer.toBlockingInputStream());
    }

    private static void logDebug(String hypothesisId, String location, String message, String datasourceId, String sourceUrl, String bucket, String key, long startMs, long contentLength, String status) {
        if (!LOG.isTraceEnabled()) {
            return;
        }
        long elapsedMs = Math.max(0L, System.currentTimeMillis() - startMs);
        String payload = String.format(
                "{\"sessionId\":\"%s\",\"runId\":\"%s\",\"hypothesisId\":\"%s\",\"timestamp\":%d,\"location\":\"%s\",\"message\":\"%s\",\"data\":{\"datasourceId\":\"%s\",\"sourceUrl\":\"%s\",\"bucket\":\"%s\",\"key\":\"%s\",\"status\":\"%s\",\"elapsedMs\":%d,\"contentLength\":%d}}",
                DEBUG_SESSION_ID,
                DEBUG_RUN_ID,
                hypothesisId,
                System.currentTimeMillis(),
                escapeJson(location),
                escapeJson(message),
                escapeJson(datasourceId),
                escapeJson(sourceUrl),
                escapeJson(bucket),
                escapeJson(key),
                escapeJson(status),
                elapsedMs,
                contentLength);
        LOG.trace(payload);
    }

    private static String errorClass(Throwable error) {
        return error == null ? "unknown" : error.getClass().getName() + ": " + String.valueOf(error.getMessage());
    }

    private static String escapeJson(String raw) {
        if (raw == null) {
            return "null";
        }
        return raw
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
