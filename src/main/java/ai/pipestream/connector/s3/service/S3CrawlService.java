package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import ai.pipestream.connector.s3.events.S3CrawlEventPublisher;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

/**
 * Service for crawling S3 buckets and emitting crawl events.
 */
@ApplicationScoped
public class S3CrawlService {

    private static final Logger LOG = Logger.getLogger(S3CrawlService.class);

    @Inject
    S3Client s3Client;

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
     * @return completion stage for the crawl operation
     */
    public CompletionStage<Void> crawlBucket(String datasourceId, String bucket, String prefix) {
        LOG.infof("Starting S3 crawl: datasourceId=%s, bucket=%s, prefix=%s", datasourceId, bucket, prefix);
        
        return crawlBucketInternal(datasourceId, bucket, prefix)
            .thenRun(() -> LOG.infof("Completed S3 crawl: datasourceId=%s, bucket=%s", datasourceId, bucket));
    }

    private CompletionStage<Void> crawlBucketInternal(String datasourceId, String bucket, String prefix) {
        String actualPrefix = (prefix != null && !prefix.isBlank()) ? prefix : config.initialCrawl().prefix();
        int maxKeys = config.initialCrawl().maxKeysPerRequest();
        
        String continuationToken = null;
        int totalObjects = 0;

        do {
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
            ListObjectsV2Response response = s3Client.listObjectsV2(request);

            for (S3Object s3Object : response.contents()) {
                S3CrawlEvent event = createCrawlEvent(datasourceId, bucket, s3Object);
                eventPublisher.publish(event);
                totalObjects++;
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        LOG.infof("Emitted %d crawl events for bucket=%s, prefix=%s", totalObjects, bucket, actualPrefix);
        return java.util.concurrent.CompletableFuture.completedFuture(null);
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
     * @return completion stage for the crawl operation
     */
    public CompletionStage<Void> crawlObject(String datasourceId, String bucket, String key) {
        LOG.infof("Crawling single S3 object: datasourceId=%s, bucket=%s, key=%s", datasourceId, bucket, key);

        try {
            // Get object metadata
            var headObjectResponse = s3Client.headObject(builder -> builder
                .bucket(bucket)
                .key(key));

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
        } catch (Exception e) {
            LOG.errorf(e, "Error crawling S3 object: bucket=%s, key=%s", bucket, key);
            return java.util.concurrent.CompletableFuture.failedFuture(e);
        }
    }
}
