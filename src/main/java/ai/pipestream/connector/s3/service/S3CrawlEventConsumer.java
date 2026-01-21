package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.client.ConnectorIntakeClient;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import io.smallrye.mutiny.Uni;
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
 * Consumer for S3 crawl events from Kafka.
 * <p>
 * This service consumes {@link S3CrawlEvent} messages from the "s3-crawl-events-in" Kafka topic
 * and processes them by downloading the corresponding S3 objects and uploading them to the
 * connector-intake-service. The processing is fully reactive and handles errors gracefully.
 * </p>
 *
 * <h2>Processing Flow</h2>
 * <ol>
 *   <li>Receive crawl event from Kafka</li>
 *   <li>Retrieve datasource configuration</li>
 *   <li>Get or create S3 client for the datasource</li>
 *   <li>Download the S3 object</li>
 *   <li>Upload the object to connector-intake-service</li>
 * </ol>
 *
 * <h2>Error Handling</h2>
 * <p>
 * Processing failures are logged but do not stop the event stream.
 * Individual event failures do not affect other events in the topic.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3CrawlEventConsumer {

    /**
     * Default constructor for CDI injection.
     */
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
     * Processes an incoming S3 crawl event from Kafka.
     * <p>
     * This method orchestrates the complete processing pipeline for a crawl event:
     * retrieving datasource configuration, downloading the S3 object, and uploading
     * it to the connector-intake-service. The method uses reactive programming
     * to handle the asynchronous operations efficiently.
     * </p>
     *
     * @param event the {@link S3CrawlEvent} protobuf message received from Kafka
     * @return a {@link Uni} that completes when event processing is finished
     * @since 1.0.0
     */
    @Incoming("s3-crawl-events-in")
    public Uni<Void> processCrawlEvent(S3CrawlEvent event) {
        LOG.infof("Processing S3 crawl event: datasourceId=%s, sourceUrl=%s", 
            event.getDatasourceId(), event.getSourceUrl());

        return datasourceConfigService.getDatasourceConfig(event.getDatasourceId())
            .flatMap(config ->
                // Get datasource-specific S3 client
                clientFactory.getOrCreateClient(config.datasourceId(), config.s3Config())
                    .flatMap(client ->
                        Uni.createFrom().completionStage(downloadObject(client, event))
                    .flatMap(s3Response -> intakeClient.uploadRaw(
                        event.getDatasourceId(),
                        config.apiKey(),
                        event.getSourceUrl(),
                        event.getBucket(),
                        event.getKey(),
                        s3Response.response().contentType(),
                        s3Response.response().contentLength(),  // Use actual size from S3, not event
                        s3Response
                    ))
                            .onFailure().invoke(error -> {
                                LOG.errorf(error, "Failed to process crawl event: datasourceId=%s, sourceUrl=%s",
                                    event.getDatasourceId(), event.getSourceUrl());
                            })
                            .replaceWithVoid()
                    )
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
}
