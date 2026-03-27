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

    /**
     * Primary key ID.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long id;

    /**
     * Datasource identifier.
     */
    @Column(name = "datasource_id", nullable = false, length = 255)
    public String datasourceId;

    /**
     * Descriptive name for the crawl target.
     */
    @Column(name = "target_name", nullable = false, length = 255)
    public String targetName;

    /**
     * S3 bucket name.
     */
    @Column(name = "bucket", nullable = false, length = 512)
    public String bucket;

    /**
     * S3 object prefix filter.
     */
    @Column(name = "object_prefix", length = 2048)
    public String objectPrefix;

    /**
     * Crawl mode (INITIAL or PERIODIC).
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "crawl_mode", nullable = false, length = 20)
    public CrawlTargetMode crawlMode = CrawlTargetMode.INITIAL;

    /**
     * Number of failures allowed per object.
     */
    @Column(name = "failure_allowance", nullable = false)
    public int failureAllowance = 1;

    /**
     * Max keys to request from S3 per batch.
     */
    @Column(name = "max_keys_per_request", nullable = false)
    public int maxKeysPerRequest = 1000;

    /**
     * Whether this crawl target is active.
     */
    @Column(name = "enabled", nullable = false)
    public boolean enabled = true;

    /**
     * Timestamp of the last crawl operation.
     */
    @Column(name = "last_crawl_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastCrawlAt;

    /**
     * Optimistic locking version.
     */
    @Version
    @Column(name = "version", nullable = false)
    public Long version;

    /**
     * Record creation timestamp.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    /**
     * Record update timestamp.
     */
    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime updatedAt;

    /**
     * Default constructor for JPA.
     */
    public CrawlTargetEntity() {
    }

    /**
     * Creates a new crawl target entity.
     *
     * @param datasourceId      datasource identifier
     * @param targetName        descriptive name
     * @param bucket            S3 bucket name
     * @param objectPrefix      S3 object prefix
     * @param crawlMode         crawl mode
     * @param failureAllowance   max failures allowed
     * @param maxKeysPerRequest max keys per S3 request
     */
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

    /**
     * Updates the record's modification timestamp.
     */
    public void touchUpdatedAt() {
        this.updatedAt = OffsetDateTime.now();
    }
}

