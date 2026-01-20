package ai.pipestream.connector.s3.events;

import ai.pipestream.apicurio.registry.protobuf.UuidKeyExtractor;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic UUID key extractor for {@link S3CrawlEvent} messages.
 * <p>
 * This extractor generates deterministic UUID keys from the datasource ID to ensure
 * that all events from the same datasource are routed to the same Kafka partition.
 * This enables proper ordering and processing of events within a datasource while
 * allowing parallel processing across different datasources.
 * </p>
 *
 * <h2>Key Generation</h2>
 * <p>
 * The UUID is generated using {@link UUID#nameUUIDFromBytes(byte[])} with the
 * datasource ID as input, ensuring the same datasource always produces the same key.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3CrawlEventUuidKeyExtractor implements UuidKeyExtractor<S3CrawlEvent> {

    /**
     * Default constructor for CDI injection.
     */
    public S3CrawlEventUuidKeyExtractor() {
    }

    /**
     * Extracts a deterministic UUID key from an {@link S3CrawlEvent}.
     * <p>
     * Generates a UUID using the event's datasource ID as the seed, ensuring
     * that all events from the same datasource receive the same partition key.
     * This enables Kafka to consistently route events from the same datasource
     * to the same partition for ordered processing.
     * </p>
     *
     * @param event the {@link S3CrawlEvent} to extract the key from
     * @return a deterministic {@link UUID} key based on the datasource ID
     * @throws IllegalArgumentException if the event is null or has an empty datasource ID
     * @since 1.0.0
     */
    @Override
    public UUID extractKey(S3CrawlEvent event) {
        if (event == null || event.getDatasourceId().isEmpty()) {
            throw new IllegalArgumentException("S3CrawlEvent must have a datasource_id");
        }
        return UUID.nameUUIDFromBytes(event.getDatasourceId().getBytes(StandardCharsets.UTF_8));
    }
}
