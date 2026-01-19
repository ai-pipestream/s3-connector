package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import ai.pipestream.connector.s3.events.S3CrawlEventPublisher;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
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
 * Service for crawling S3 buckets and emitting crawl events.
 */
@ApplicationScoped
public class S3CrawlService {

    private static final Logger LOG = Logger.getLogger(S3CrawlService.class);

    @Inject
    S3AsyncClient s3AsyncClient;

    @Inject
    S3CrawlEventPublisher eventPublisher;

    @Inject
    S3ConnectorConfig config;

    /**
     * Perform initial/recrawl of an S3 bucket.
     * Lists all objects matching the prefix and emits crawl events for each.
     *
     * @param datasourceId datasource identifier
     * @param bucket bucket name
     * @param prefix optional prefix filter (may be null/empty)
     * @return reactive Uni for the crawl operation
     */
    public Uni<Void> crawlBucket(String datasourceId, String bucket, String prefix) {
        LOG.infof("Starting S3 crawl: datasourceId=%s, bucket=%s, prefix=%s", datasourceId, bucket, prefix);

        AtomicInteger totalObjects = new AtomicInteger(0);
        return crawlBucketInternal(datasourceId, bucket, prefix, null, totalObjects)
            .invoke(() -> LOG.infof("Completed S3 crawl: datasourceId=%s, bucket=%s", datasourceId, bucket))
            .replaceWithVoid();
    }

    private Uni<Void> crawlBucketInternal(String datasourceId,
                                          String bucket,
                                          String prefix,
                                          String continuationToken,
                                          AtomicInteger totalObjects) {
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

        return Uni.createFrom().completionStage(s3AsyncClient.listObjectsV2(request))
            .flatMap(response -> handleListResponse(datasourceId, bucket, actualPrefix, response, totalObjects));
    }

    private Uni<Void> handleListResponse(String datasourceId,
                                         String bucket,
                                         String actualPrefix,
                                         ListObjectsV2Response response,
                                         AtomicInteger totalObjects) {
        List<S3Object> contents = response.contents();
        
        if (contents.isEmpty()) {
            String nextToken = response.nextContinuationToken();
            if (nextToken == null) {
                return Uni.createFrom().voidItem()
                    .invoke(() -> LOG.infof("Emitted %d crawl events for bucket=%s, prefix=%s",
                        totalObjects.get(), bucket, actualPrefix));
            }
            return crawlBucketInternal(datasourceId, bucket, actualPrefix, nextToken, totalObjects);
        }

        // Convert each S3Object to a Uni<Void> for publishing
        List<Uni<Void>> publishUnis = contents.stream()
            .map(s3Object -> {
                S3CrawlEvent event = createCrawlEvent(datasourceId, bucket, s3Object);
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
            .flatMap(ignored -> crawlBucketInternal(datasourceId, bucket, actualPrefix, nextToken, totalObjects));
    }

    private S3CrawlEvent createCrawlEvent(String datasourceId, String bucket, S3Object s3Object) {
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
            lastModified
        );
    }

    /**
     * Crawl a single S3 object (for event-driven mode or manual triggers).
     *
     * @param datasourceId datasource identifier
     * @param bucket bucket name
     * @param key object key
     * @return reactive Uni for the crawl operation
     */
    public Uni<Void> crawlObject(String datasourceId, String bucket, String key) {
        LOG.infof("Crawling single S3 object: datasourceId=%s, bucket=%s, key=%s", datasourceId, bucket, key);

        return Uni.createFrom().completionStage(
                s3AsyncClient.headObject(builder -> builder
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
                    headObjectResponse.lastModified()
                );
                return eventPublisher.publish(event);
            })
            .onFailure().invoke(error -> {
                LOG.errorf(error, "Error crawling S3 object: bucket=%s, key=%s", bucket, key);
            });
    }
}
