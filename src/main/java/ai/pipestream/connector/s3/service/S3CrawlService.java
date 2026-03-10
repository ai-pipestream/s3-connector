package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import ai.pipestream.connector.s3.events.S3CrawlEventPublisher;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import ai.pipestream.connector.s3.state.CrawlSource;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service for crawling S3 buckets and emitting crawl events to Kafka.
 * <p>
 * This service provides the core crawling functionality for the S3 connector,
 * supporting both initial bucket crawls and individual object processing.
 * It discovers S3 objects, creates crawl events, and publishes them to Kafka
 * for downstream processing by the connector-intake-service.
 * </p>
 *
 * <h2>Crawl Modes</h2>
 * <ul>
 *   <li><strong>Bucket Crawl</strong>: Lists all objects in a bucket with optional prefix filtering</li>
 *   <li><strong>Object Crawl</strong>: Processes a single S3 object with full metadata</li>
 * </ul>
 *
 * <h2>Pagination</h2>
 * <p>
 * Bucket crawling uses S3's ListObjectsV2 API with continuation tokens to handle
 * buckets with large numbers of objects. Objects are processed in batches based
 * on the configured {@code maxKeysPerRequest} setting.
 * </p>
 *
 * <h2>Event Publishing</h2>
 * <p>
 * Each discovered object generates an {@link S3CrawlEvent} that is published to
 * the "s3-crawl-events-out" Kafka topic for consumption by the event processing pipeline.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3CrawlService {

    /**
     * Default constructor for CDI injection.
     */
    public S3CrawlService() {
    }

    private static final Logger LOG = Logger.getLogger(S3CrawlService.class);

    @Inject
    S3ClientFactory clientFactory;

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Inject
    S3CrawlEventPublisher eventPublisher;

    @Inject
    S3ConnectorConfig config;

    /**
     * Performs a complete crawl of an S3 bucket and emits crawl events for all discovered objects.
     * <p>
     * This method lists all objects in the specified bucket that match the optional prefix filter
     * and emits a {@link S3CrawlEvent} for each object to Kafka. The crawl uses pagination to
     * handle buckets with large numbers of objects efficiently.
     * </p>
     *
     * <h4>Prefix Resolution</h4>
     * <p>
     * The prefix parameter takes precedence over the configured prefix in {@link S3ConnectorConfig}.
     * If both are null or empty, all objects in the bucket are crawled.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param bucket the name of the S3 bucket to crawl
     * @param prefix optional prefix filter for objects (may be null or empty)
     * @return a {@link Uni} that completes when all objects have been processed and events emitted
     * @since 1.0.0
     */
    public Uni<Void> crawlBucket(String datasourceId, String bucket, String prefix) {
        return crawlBucket(datasourceId, bucket, prefix, CrawlSource.INITIAL);
    }

    /**
     * Crawls an S3 bucket and emits crawl events for discovered objects.
     *
     * @param datasourceId unique identifier for the datasource
     * @param bucket       S3 bucket name
     * @param prefix       optional prefix filter (may be {@code null})
     * @param crawlSource  whether this is an initial or incremental crawl
     * @return a Uni that completes when all objects have been processed
     */
    public Uni<Void> crawlBucket(String datasourceId, String bucket, String prefix, CrawlSource crawlSource) {
        LOG.infof("Starting S3 crawl: datasourceId=%s, bucket=%s, prefix=%s", datasourceId, bucket, prefix);

        AtomicInteger totalObjects = new AtomicInteger(0);

        return datasourceConfigService.getDatasourceConfig(datasourceId)
            .flatMap(datasourceConfig -> clientFactory.getOrCreateClient(datasourceId, datasourceConfig.s3Config())
                .flatMap(client -> {
                    String configuredPrefix = config.initialCrawl().prefix().orElse(null);
                    String actualPrefix = (prefix != null && !prefix.isBlank()) ? prefix : configuredPrefix;
                    int maxKeys = config.initialCrawl().maxKeysPerRequest();

                    ListObjectsV2Request firstRequest = ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .maxKeys(maxKeys)
                        .prefix(actualPrefix)
                        .build();

                    return crawlPage(client, firstRequest, datasourceId, bucket, crawlSource, totalObjects)
                        .invoke(() -> LOG.infof("Completed S3 crawl: emitted %d events for bucket=%s, prefix=%s",
                            totalObjects.get(), bucket, actualPrefix));
                }));
    }

    /**
     * Fetches one page of S3 objects, publishes all events for that page concurrently,
     * then recursively processes the next page if one exists. Fully reactive — no blocking.
     */
    private Uni<Void> crawlPage(S3AsyncClient client, ListObjectsV2Request request,
                                String datasourceId, String bucket,
                                CrawlSource crawlSource, AtomicInteger totalObjects) {
        return Uni.createFrom().completionStage(client.listObjectsV2(request))
            .flatMap(page -> {
                // Publish all events in this page as a concurrent batch
                List<Uni<Void>> pageEvents = page.contents().stream()
                    .map(s3Object -> {
                        totalObjects.incrementAndGet();
                        return eventPublisher.publish(createCrawlEvent(datasourceId, bucket, s3Object, crawlSource));
                    })
                    .collect(Collectors.toList());

                Uni<Void> publishPage = pageEvents.isEmpty()
                        ? Uni.createFrom().voidItem()
                        : Uni.combine().all().unis(pageEvents).discardItems();

                // After the page is fully published, move to the next page if there is one
                if (Boolean.TRUE.equals(page.isTruncated())) {
                    ListObjectsV2Request nextRequest = request.toBuilder()
                        .continuationToken(page.nextContinuationToken())
                        .build();
                    return publishPage.flatMap(ignored -> crawlPage(client, nextRequest, datasourceId, bucket, crawlSource, totalObjects));
                }
                return publishPage;
            });
    }

    private S3CrawlEvent createCrawlEvent(String datasourceId, String bucket, S3Object s3Object, CrawlSource crawlSource) {
        Instant lastModified = s3Object.lastModified() != null 
            ? s3Object.lastModified() 
            : Instant.now();

        // Note: S3Object from ListObjectsV2 doesn't include versionId
        // Version ID is only available when getting/heading specific objects
        // For initial crawl, we'll process objects without version IDs
        String versionId = null;

        return eventPublisher.buildEvent(
            datasourceId,
            bucket,
            s3Object.key(),
            versionId,
            s3Object.size(),
            s3Object.eTag(),
            lastModified,
            crawlSource
        );
    }

    /**
     * Crawls a single S3 object and emits a crawl event for it.
     * <p>
     * This method retrieves complete metadata for a specific S3 object using the
     * HeadObject API (which includes version ID information not available in list operations)
     * and emits a single {@link S3CrawlEvent} to Kafka.
     * </p>
     *
     * <h4>Use Cases</h4>
     * <ul>
     *   <li>Event-driven crawling triggered by S3 notifications</li>
     *   <li>Manual processing of specific objects</li>
     *   <li>Re-processing of individual objects</li>
     * </ul>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param bucket the name of the S3 bucket containing the object
     * @param key the S3 object key to crawl
     * @return a {@link Uni} that completes when the object has been processed and event emitted
     * @since 1.0.0
     */
    public Uni<Void> crawlObject(String datasourceId, String bucket, String key) {
        return crawlObject(datasourceId, bucket, key, CrawlSource.INCREMENTAL);
    }

    /**
     * Crawls a single S3 object and emits a crawl event for it.
     *
     * @param datasourceId unique identifier for the datasource
     * @param bucket       S3 bucket name
     * @param key          S3 object key
     * @param crawlSource  whether this is an initial or incremental crawl
     * @return a Uni that completes when the object has been processed
     */
    public Uni<Void> crawlObject(String datasourceId, String bucket, String key, CrawlSource crawlSource) {
        LOG.infof("Crawling single S3 object: datasourceId=%s, bucket=%s, key=%s", datasourceId, bucket, key);

        return datasourceConfigService.getDatasourceConfig(datasourceId)
            .flatMap(datasourceConfig ->
                // Get datasource-specific S3 client
                clientFactory.getOrCreateClient(datasourceId, datasourceConfig.s3Config())
                    .flatMap(client ->
                        Uni.createFrom().completionStage(
                                client.headObject(builder -> builder
                                    .bucket(bucket)
                                    .key(key)))
                    .flatMap(headObjectResponse -> {
                        S3CrawlEvent event = eventPublisher.buildEvent(
                            datasourceId,
                            bucket,
                            key,
                            headObjectResponse.versionId(),
                            headObjectResponse.contentLength(),
                            headObjectResponse.eTag(),
                            headObjectResponse.lastModified(),
                            crawlSource
                        );
                        return eventPublisher.publish(event);
                    })
                            .onFailure().invoke(error -> {
                                LOG.errorf(error, "Error crawling S3 object: bucket=%s, key=%s", bucket, key);
                            })
                    )
            );
    }
}
