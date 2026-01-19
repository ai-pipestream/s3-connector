package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.client.ConnectorIntakeClient;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Consumer for S3 crawl events.
 * <p>
 * Processes crawl events by downloading S3 objects and uploading them to connector-intake-service.
 * The extension auto-detects the protobuf message type from the parameter.
 */
@ApplicationScoped
public class S3CrawlEventConsumer {

    private static final Logger LOG = Logger.getLogger(S3CrawlEventConsumer.class);

    @Inject
    S3Client s3Client;

    @Inject
    ConnectorIntakeClient intakeClient;

    @Inject
    DatasourceConfigService datasourceConfigService;

    /**
     * Process an S3 crawl event.
     * <p>
     * Downloads the S3 object and uploads it to connector-intake-service via HTTP multipart upload.
     *
     * @param event S3 crawl event protobuf message
     */
    @Incoming("s3-crawl-events")
    public Uni<Void> processCrawlEvent(S3CrawlEvent event) {
        LOG.infof("Processing S3 crawl event: datasourceId=%s, sourceUrl=%s", 
            event.getDatasourceId(), event.getSourceUrl());

        return datasourceConfigService.getDatasourceConfig(event.getDatasourceId())
            .flatMap(config -> {
                // Download S3 object
                ResponseInputStream<GetObjectResponse> s3Response = downloadObject(event);
                
                // Upload to connector-intake-service
                return intakeClient.uploadRaw(
                    event.getDatasourceId(),
                    config.apiKey(),
                    event.getSourceUrl(),
                    event.getBucket(),
                    event.getKey(),
                    s3Response.response().contentType(),
                    event.getSizeBytes(),
                    s3Response
                ).onFailure().invoke(error -> {
                    LOG.errorf(error, "Failed to process crawl event: datasourceId=%s, sourceUrl=%s",
                        event.getDatasourceId(), event.getSourceUrl());
                }).replaceWithVoid();
            });
    }

    private ResponseInputStream<GetObjectResponse> downloadObject(S3CrawlEvent event) {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
            .bucket(event.getBucket())
            .key(event.getKey());

        if (event.getVersionId() != null && !event.getVersionId().isEmpty()) {
            requestBuilder.versionId(event.getVersionId());
        }

        GetObjectRequest request = requestBuilder.build();
        return s3Client.getObject(request);
    }
}
