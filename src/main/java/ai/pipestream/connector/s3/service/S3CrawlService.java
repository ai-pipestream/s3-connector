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
        // Fetch config once (in Vert.x context) and pass it through
        return datasourceConfigService.getDatasourceConfig(datasourceId)
            .flatMap(datasourceConfig ->
                crawlBucketInternal(datasourceId, datasourceConfig, bucket, prefix, null, crawlSource, totalObjects)
                    .invoke(() -> LOG.infof("Completed S3 crawl: datasourceId=%s, bucket=%s", datasourceId, bucket))
                    .replaceWithVoid()
            );
    }

    private Uni<Void> crawlBucketInternal(String datasourceId,
                                          DatasourceConfigService.DatasourceConfig datasourceConfig,
                                          String bucket,
                                          String prefix,
                                          String continuationToken,
                                          CrawlSource crawlSource,
                                          AtomicInteger totalObjects) {
        return
                // Get datasource-specific S3 client
                clientFactory.getOrCreateClient(datasourceId, datasourceConfig.s3Config())
                    .flatMap(client -> {
                                String configuredPrefix = config.initialCrawl().prefix().orElse(null);
                        String actualPrefix = (prefix != null && !prefix.isBlank()) ? prefix : configuredPrefix;
                        int maxKeys = config.initialCrawl().maxKeysPerRequest();

                        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                            .bucket(bucket)
                            .maxKeys(maxKeys);

                        if (actualPrefix != null && !actualPrefix.isBlank()) {
                            requestBuilder.prefix(actualPrefix);
                        }

                        if (continuationToken != null) {
                            requestBuilder.continuationToken(continuationToken);
                        }

                        ListObjectsV2Request request = requestBuilder.build();

                        return Uni.createFrom().completionStage(client.listObjectsV2(request))
                            .flatMap(response -> handleListResponse(datasourceId, datasourceConfig, bucket, actualPrefix, response, crawlSource, totalObjects));
                    });
    }

    private Uni<Void> handleListResponse(String datasourceId,
                                         DatasourceConfigService.DatasourceConfig datasourceConfig,
                                         String bucket,
                                         String actualPrefix,
                                         ListObjectsV2Response response,
                                         CrawlSource crawlSource,
                                         AtomicInteger totalObjects) {
        List<S3Object> contents = response.contents();
        
        if (contents.isEmpty()) {
            String nextToken = response.nextContinuationToken();
            if (nextToken == null) {
                return Uni.createFrom().voidItem()
                    .invoke(() -> LOG.infof("Emitted %d crawl events for bucket=%s, prefix=%s",
                        totalObjects.get(), bucket, actualPrefix));
            }
            return crawlBucketInternal(datasourceId, datasourceConfig, bucket, actualPrefix, nextToken, crawlSource, totalObjects);
        }

        // Convert each S3Object to a Uni<Void> for publishing
        List<Uni<Void>> publishUnis = contents.stream()
            .map(s3Object -> {
                S3CrawlEvent event = createCrawlEvent(datasourceId, bucket, s3Object, crawlSource);
                totalObjects.incrementAndGet();
                return eventPublisher.publish(event);
            })
            .collect(Collectors.toList());

        // Combine all publish operations into a single Uni
        Uni<Void> sendAll = Uni.combine().all().unis(publishUnis).discardItems();

        String nextToken = response.nextContinuationToken();
        if (nextToken == null) {
            return sendAll
                .invoke(() -> LOG.infof("Emitted %d crawl events for bucket=%s, prefix=%s",
                    totalObjects.get(), bucket, actualPrefix))
                .replaceWithVoid();
        }

        return sendAll
            .flatMap(ignored -> crawlBucketInternal(datasourceId, datasourceConfig, bucket, actualPrefix, nextToken, crawlSource, totalObjects));
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
