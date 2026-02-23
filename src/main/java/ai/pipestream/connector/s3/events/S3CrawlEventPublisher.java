package ai.pipestream.connector.s3.events;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import ai.pipestream.connector.s3.state.CrawlSource;
import com.google.protobuf.Timestamp;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Publisher for S3 crawl events to Kafka using Protobuf serialization.
 * <p>
 * This service publishes {@link S3CrawlEvent} messages to Kafka topics for downstream
 * processing by the pipeline. Events are serialized using Protocol Buffers and
 * sent through the Apicurio Registry integration for schema management.
 * </p>
 *
 * <h2>Event Publishing</h2>
 * <p>
 * Events are published asynchronously using Mutiny's reactive programming model.
 * Each event represents a discovered S3 object that should be processed by the pipeline.
 * </p>
 *
 * <h2>Event ID Generation</h2>
 * <p>
 * Event IDs are deterministically generated using SHA-256 hashing of key metadata
 * (datasource ID, bucket, key, version ID, and timestamp) to ensure idempotent processing.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3CrawlEventPublisher {

    /**
     * Default constructor for CDI injection.
     */
    public S3CrawlEventPublisher() {
    }

    private static final Logger LOG = Logger.getLogger(S3CrawlEventPublisher.class);

    @Inject
    @ProtobufChannel("s3-crawl-events-out")
    ProtobufEmitter<S3CrawlEvent> eventEmitter;

    /**
     * Publishes an S3 crawl event to the configured Kafka topic.
     * <p>
     * The event is serialized using Protocol Buffers and sent asynchronously
     * to the "s3-crawl-events-out" channel. If publishing fails, the error
     * is logged but does not throw an exception to avoid interrupting crawl operations.
     * </p>
     *
     * @param event the {@link S3CrawlEvent} protobuf message to publish
     * @return a {@link Uni} that completes when the event has been sent,
     *         or fails if the send operation encounters an error
     * @since 1.0.0
     */
    public Uni<Void> publish(S3CrawlEvent event) {
        LOG.debugf("Publishing S3 crawl event: datasourceId=%s, sourceUrl=%s", 
            event.getDatasourceId(), event.getSourceUrl());
        
        return Uni.createFrom().completionStage(eventEmitter.send(event))
            .onFailure().invoke(error -> {
                LOG.errorf(error, "Error publishing S3 crawl event: datasourceId=%s, sourceUrl=%s",
                    event.getDatasourceId(), event.getSourceUrl());
            })
            .replaceWithVoid();
    }

    /**
     * Builds an {@link S3CrawlEvent} protobuf message from S3 object metadata.
     * <p>
     * Creates a complete crawl event with all required fields including a
     * deterministically generated event ID and properly formatted source URL.
     * The event timestamp is set to the current time when this method is called.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param bucket the S3 bucket name containing the object
     * @param key the S3 object key (path within the bucket)
     * @param versionId the S3 version ID for versioned objects, may be null
     * @param sizeBytes the size of the object in bytes
     * @param etag the S3 ETag for the object
     * @param lastModified the last modified timestamp of the object
     * @return a fully constructed {@link S3CrawlEvent} protobuf message
     * @since 1.0.0
     */
    public S3CrawlEvent buildEvent(String datasourceId, String bucket, String key,
                                   String versionId, long sizeBytes, String etag, Instant lastModified) {
        return buildEvent(datasourceId, bucket, key, versionId, sizeBytes, etag, lastModified, CrawlSource.INCREMENTAL);
    }

    /**
     * Builds an {@link S3CrawlEvent} protobuf message from S3 object metadata with source classification.
     *
     * @param datasourceId the unique identifier for the datasource
     * @param bucket the S3 bucket name containing the object
     * @param key the S3 object key (path within the bucket)
     * @param versionId the S3 version ID for versioned objects, may be null
     * @param sizeBytes the size of the object in bytes
     * @param etag the S3 ETag for the object
     * @param lastModified the last modified timestamp of the object
     * @param crawlSource the crawl source classification
     * @return a fully constructed {@link S3CrawlEvent} protobuf message
     * @since 1.0.0
     */
    public S3CrawlEvent buildEvent(String datasourceId, String bucket, String key,
                                   String versionId, long sizeBytes, String etag, Instant lastModified,
                                   CrawlSource crawlSource) {
        Instant now = Instant.now();
        String eventId = computeEventId(datasourceId, bucket, key, versionId, now);
        
        // Build source URL
        String sourceUrl = "s3://" + bucket + "/" + key;
        if (versionId != null && !versionId.isBlank()) {
            sourceUrl += "?versionId=" + versionId;
        }

        CrawlSource resolvedCrawlSource = crawlSource == null ? CrawlSource.INCREMENTAL : crawlSource;
        sourceUrl = appendSourceMarker(sourceUrl, "crawl_source", resolvedCrawlSource.name().toLowerCase());

        return S3CrawlEvent.newBuilder()
            .setEventId(eventId)
            .setTimestamp(toProtoTimestamp(now))
            .setDatasourceId(datasourceId)
            .setBucket(bucket)
            .setKey(key)
            .setVersionId(versionId != null && !versionId.isBlank() ? versionId : "")
            .setSizeBytes(sizeBytes)
            .setEtag(etag != null && !etag.isBlank() ? etag : "")
            .setLastModified(toProtoTimestamp(lastModified))
            .setSourceUrl(sourceUrl)
            .build();
    }

    private static String appendSourceMarker(String sourceUrl, String key, String value) {
        if (sourceUrl == null || key == null || key.isBlank() || value == null || value.isBlank()) {
            return sourceUrl;
        }

        if (sourceUrl.contains("?")) {
            return sourceUrl + "&" + key + "=" + value;
        }
        return sourceUrl + "?" + key + "=" + value;
    }

    /**
     * Compute deterministic event ID: first 32 chars of SHA-256(datasourceId + bucket + key + versionId + timestamp_millis).
     */
    private static String computeEventId(String datasourceId, String bucket, String key, String versionId, Instant timestamp) {
        String input = datasourceId + ":" + bucket + ":" + key + ":" + (versionId != null ? versionId : "") + ":" + timestamp.toEpochMilli();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is always available
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private static Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
}
