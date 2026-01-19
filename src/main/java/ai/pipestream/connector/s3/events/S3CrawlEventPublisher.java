package ai.pipestream.connector.s3.events;

import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.CompletionStage;

/**
 * Publisher for S3 crawl events to Kafka.
 * <p>
 * Uses Protobuf serialization via Apicurio Registry.
 */
@ApplicationScoped
public class S3CrawlEventPublisher {

    private static final Logger LOG = Logger.getLogger(S3CrawlEventPublisher.class);

    @Inject
    @ProtobufChannel("s3-crawl-events")
    ProtobufEmitter<S3CrawlEvent> eventEmitter;

    /**
     * Publish an S3 crawl event.
     *
     * @param event the crawl event protobuf message to publish
     * @return a {@link CompletionStage} handle for the send operation
     */
    public CompletionStage<Void> publish(S3CrawlEvent event) {
        try {
            LOG.debugf("Publishing S3 crawl event: datasourceId=%s, sourceUrl=%s", 
                event.getDatasourceId(), event.getSourceUrl());
            return eventEmitter.send(event);
        } catch (Exception e) {
            LOG.errorf(e, "Error publishing S3 crawl event: datasourceId=%s, sourceUrl=%s",
                event.getDatasourceId(), event.getSourceUrl());
            throw new RuntimeException("Failed to publish S3 crawl event", e);
        }
    }

    /**
     * Build an S3CrawlEvent from S3 object metadata.
     *
     * @param datasourceId datasource identifier
     * @param bucket S3 bucket name
     * @param key S3 object key
     * @param versionId S3 version ID (may be null)
     * @param sizeBytes object size in bytes
     * @param etag object ETag
     * @param lastModified last modified timestamp
     * @return S3CrawlEvent protobuf message
     */
    public S3CrawlEvent buildEvent(String datasourceId, String bucket, String key,
                                   String versionId, long sizeBytes, String etag, Instant lastModified) {
        Instant now = Instant.now();
        String eventId = computeEventId(datasourceId, bucket, key, versionId, now);
        
        // Build source URL
        String sourceUrl = "s3://" + bucket + "/" + key;
        if (versionId != null && !versionId.isBlank()) {
            sourceUrl += "?versionId=" + versionId;
        }

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
