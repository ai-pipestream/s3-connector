package ai.pipestream.connector.s3.events;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic UUID key extractor for S3CrawlEvent.
 * <p>
 * Generates deterministic UUIDs from datasource_id for proper Kafka partitioning.
 */
@ApplicationScoped
public class S3CrawlEventUuidKeyExtractor implements UuidKeyExtractor<S3CrawlEvent> {

    /**
     * Extract deterministic UUID key from S3CrawlEvent.
     * <p>
     * Uses datasource_id as the base for deterministic UUID generation.
     *
     * @param event the S3 crawl event
     * @return deterministic UUID key
     */
    @Override
    public UUID extractKey(S3CrawlEvent event) {
        if (event == null || event.getDatasourceId().isEmpty()) {
            throw new IllegalArgumentException("S3CrawlEvent must have a datasource_id");
        }
        return UUID.nameUUIDFromBytes(event.getDatasourceId().getBytes(StandardCharsets.UTF_8));
    }
}
