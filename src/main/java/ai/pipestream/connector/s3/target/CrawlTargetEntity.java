package ai.pipestream.connector.s3.target;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;

import java.time.OffsetDateTime;

/**
 * Database row for a configured S3 crawl target.
 */
@Entity
@Table(
    name = "s3_crawl_targets",
    indexes = {
        @Index(name = "idx_s3_crawl_targets_datasource", columnList = "datasource_id"),
        @Index(name = "idx_s3_crawl_targets_bucket", columnList = "bucket"),
        @Index(name = "idx_s3_crawl_targets_enabled", columnList = "enabled"),
        @Index(name = "idx_s3_crawl_targets_mode", columnList = "crawl_mode")
    },
    uniqueConstraints = @UniqueConstraint(
        name = "uk_s3_crawl_targets_bucket_prefix",
        columnNames = {"datasource_id", "bucket", "object_prefix"}
    )
)
public class CrawlTargetEntity extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long id;

    @Column(name = "datasource_id", nullable = false, length = 255)
    public String datasourceId;

    @Column(name = "target_name", nullable = false, length = 255)
    public String targetName;

    @Column(name = "bucket", nullable = false, length = 512)
    public String bucket;

    @Column(name = "object_prefix", length = 2048)
    public String objectPrefix;

    @Enumerated(EnumType.STRING)
    @Column(name = "crawl_mode", nullable = false, length = 20)
    public CrawlTargetMode crawlMode = CrawlTargetMode.INITIAL;

    @Column(name = "failure_allowance", nullable = false)
    public int failureAllowance = 1;

    @Column(name = "max_keys_per_request", nullable = false)
    public int maxKeysPerRequest = 1000;

    @Column(name = "enabled", nullable = false)
    public boolean enabled = true;

    @Column(name = "last_crawl_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastCrawlAt;

    @Version
    @Column(name = "version", nullable = false)
    public Long version;

    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime updatedAt;

    public CrawlTargetEntity() {
    }

    public CrawlTargetEntity(
        String datasourceId,
        String targetName,
        String bucket,
        String objectPrefix,
        CrawlTargetMode crawlMode,
        int failureAllowance,
        int maxKeysPerRequest
    ) {
        this.datasourceId = datasourceId;
        this.targetName = targetName;
        this.bucket = bucket;
        this.objectPrefix = objectPrefix;
        this.crawlMode = crawlMode != null ? crawlMode : CrawlTargetMode.INITIAL;
        this.failureAllowance = failureAllowance;
        this.maxKeysPerRequest = maxKeysPerRequest;
        this.enabled = true;
        OffsetDateTime now = OffsetDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void touchUpdatedAt() {
        this.updatedAt = OffsetDateTime.now();
    }
}

