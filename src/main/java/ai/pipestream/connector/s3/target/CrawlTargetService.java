package ai.pipestream.connector.s3.target;

import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.List;

/**
 * Service for CRUD operations on crawl targets.
 */
@ApplicationScoped
public class CrawlTargetService {
    private static final Logger LOG = Logger.getLogger(CrawlTargetService.class);

    public CrawlTargetService() {
    }

    /**
     * Immutable input model for crawl target operations.
     */
    public record CrawlTargetSpec(
        String datasourceId,
        String targetName,
        String bucket,
        String objectPrefix,
        CrawlTargetMode mode,
        int failureAllowance,
        int maxKeysPerRequest
    ) {
        public CrawlTargetSpec {
            if (datasourceId == null || datasourceId.isBlank()) {
                throw new IllegalArgumentException("datasourceId is required");
            }
            if (targetName == null || targetName.isBlank()) {
                throw new IllegalArgumentException("targetName is required");
            }
            if (bucket == null || bucket.isBlank()) {
                throw new IllegalArgumentException("bucket is required");
            }
            if (failureAllowance < 0) {
                throw new IllegalArgumentException("failureAllowance must be 0 or greater");
            }
            if (mode == null) {
                throw new IllegalArgumentException("mode is required");
            }
            if (maxKeysPerRequest <= 0) {
                throw new IllegalArgumentException("maxKeysPerRequest must be greater than 0");
            }
        }
    }

    @WithTransaction
    public Uni<CrawlTargetEntity> createTarget(CrawlTargetSpec spec) {
        LOG.debugf("Creating crawl target for datasource=%s, bucket=%s", spec.datasourceId(), spec.bucket());

        CrawlTargetEntity target = new CrawlTargetEntity(
            spec.datasourceId(),
            spec.targetName(),
            spec.bucket(),
            normalizePrefix(spec.objectPrefix()),
            spec.mode(),
            spec.failureAllowance(),
            spec.maxKeysPerRequest()
        );

        return target.persist()
            .replaceWith(target);
    }

    @WithTransaction
    public Uni<CrawlTargetEntity> getTarget(Long id) {
        if (id == null || id <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("id must be a positive number"));
        }
        return CrawlTargetEntity.<CrawlTargetEntity>findById(id)
            .onItem().ifNull().failWith(
                () -> new IllegalStateException("No crawl target found for id=" + id)
            );
    }

    @WithTransaction
    public Uni<List<CrawlTargetEntity>> listTargetsForDatasource(String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("datasourceId is required"));
        }
        return CrawlTargetEntity.find("datasourceId", datasourceId).list();
    }

    @WithTransaction
    public Uni<CrawlTargetEntity> updateTarget(Long id, CrawlTargetSpec spec) {
        if (spec == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("spec is required"));
        }
        return getTarget(id).flatMap(target -> {
            applySpec(target, spec);
            return Uni.createFrom().item(target);
        });
    }

    @WithTransaction
    public Uni<Void> deleteTarget(Long id) {
        if (id == null || id <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("id must be a positive number"));
        }
        return CrawlTargetEntity.deleteById(id)
            .flatMap(deleted -> deleted
                ? Uni.createFrom().voidItem()
                : Uni.createFrom().failure(new IllegalStateException("No crawl target found for id=" + id)))
            .replaceWithVoid();
    }

    public static String normalizePrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return null;
        }
        return prefix;
    }

    private static void applySpec(CrawlTargetEntity target, CrawlTargetSpec spec) {
        target.datasourceId = spec.datasourceId();
        target.targetName = spec.targetName();
        target.bucket = spec.bucket();
        target.objectPrefix = normalizePrefix(spec.objectPrefix());
        target.crawlMode = spec.mode() != null ? spec.mode() : CrawlTargetMode.INITIAL;
        target.failureAllowance = spec.failureAllowance();
        target.maxKeysPerRequest = spec.maxKeysPerRequest();
        target.touchUpdatedAt();
        target.lastCrawlAt = null;
        target.enabled = true;
    }
}

