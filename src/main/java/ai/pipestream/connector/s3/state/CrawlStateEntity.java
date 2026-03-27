package ai.pipestream.connector.s3.state;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;

/**
 * Persistent state for S3 object crawl activity.
 * <p>
 * This table tracks discovered objects and their processing state so crawl runs can
 * be incremental and resumable.
 */
@Entity
@Table(
    name = "s3_crawl_state",
    indexes = {
        @Index(name = "idx_s3_crawl_state_datasource_bucket", columnList = "datasource_id,bucket"),
        @Index(name = "idx_s3_crawl_state_status", columnList = "status"),
        @Index(name = "idx_s3_crawl_state_retry_at", columnList = "next_retry_at"),
        @Index(name = "idx_s3_crawl_state_updated_at", columnList = "updated_at")
    },
    uniqueConstraints = @UniqueConstraint(
        name = "uk_s3_crawl_state_object",
        columnNames = {"datasource_id", "bucket", "object_key", "object_version_id"}
    )
)
public class CrawlStateEntity extends PanacheEntityBase {

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
     * S3 bucket name.
     */
    @Column(name = "bucket", nullable = false, length = 512)
    public String bucket;

    /**
     * S3 object key.
     */
    @Column(name = "object_key", nullable = false, length = 4096)
    public String objectKey;

    /**
     * S3 object version ID.
     */
    @Column(name = "object_version_id", nullable = false, length = 255)
    public String objectVersionId;

    /**
     * S3 object ETag.
     */
    @Column(name = "object_etag", nullable = true, length = 255)
    public String objectEtag;

    /**
     * Object size in bytes.
     */
    @Column(name = "size_bytes", nullable = false)
    public long sizeBytes;

    /**
     * Object last modified timestamp.
     */
    @Column(name = "last_modified", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastModified;

    /**
     * Current crawl status.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    public CrawlStateStatus status = CrawlStateStatus.PENDING;

    /**
     * Number of attempts made to process this object.
     */
    @Column(name = "attempt_count", nullable = false)
    public int attemptCount;

    /**
     * Number of failures allowed before marking as exhausted.
     */
    @Column(name = "failure_allowance", nullable = false)
    public int failureAllowance = 1;

    /**
     * Source of the crawl activity.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "crawl_source", nullable = false, length = 20)
    public CrawlSource crawlSource = CrawlSource.INCREMENTAL;

    /**
     * Scheduled time for next retry after failure.
     */
    @Column(name = "next_retry_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime nextRetryAt;

    /**
     * Timestamp of the last processing attempt.
     */
    @Column(name = "last_attempt_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastAttemptAt;

    /**
     * Timestamp of successful completion.
     */
    @Column(name = "completed_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime completedAt;

    /**
     * Object fingerprint for change detection.
     */
    @Column(name = "fingerprint", length = 255)
    public String fingerprint;

    /**
     * Last error message encountered during processing.
     */
    @Column(name = "last_error", columnDefinition = "TEXT")
    public String lastError;

    /**
     * Record update timestamp.
     */
    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime updatedAt;

    /**
     * Record creation timestamp.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    /**
     * Default constructor for JPA.
     */
    public CrawlStateEntity() {
    }

    /**
     * Creates a new crawl state for an S3 object.
     *
     * @param datasourceId    datasource identifier
     * @param bucket          S3 bucket name
     * @param objectKey       S3 object key
     * @param objectVersionId S3 object version ID
     * @param objectEtag      S3 object ETag
     * @param sizeBytes       object size in bytes
     * @param lastModified    object last modified timestamp
     * @param fingerprint     object fingerprint
     */
    public CrawlStateEntity(
        String datasourceId,
        String bucket,
        String objectKey,
        String objectVersionId,
        String objectEtag,
        long sizeBytes,
        OffsetDateTime lastModified,
        String fingerprint
    ) {
        this.datasourceId = datasourceId;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.objectVersionId = objectVersionId == null ? "" : objectVersionId;
        this.objectEtag = objectEtag;
        this.sizeBytes = sizeBytes;
        this.lastModified = lastModified;
        this.fingerprint = fingerprint;
        this.status = CrawlStateStatus.PENDING;
        this.attemptCount = 0;
        this.failureAllowance = 1;
        this.crawlSource = CrawlSource.INCREMENTAL;
        OffsetDateTime now = OffsetDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    /**
     * Marks a processing attempt as started.
     */
    public void markAttemptStarted() {
        this.attemptCount++;
        this.lastAttemptAt = OffsetDateTime.now();
        this.status = CrawlStateStatus.IN_PROGRESS;
        this.updatedAt = this.lastAttemptAt;
        this.lastError = null;
    }

    /**
     * Marks the object as successfully processed.
     */
    public void markCompleted() {
        OffsetDateTime now = OffsetDateTime.now();
        this.completedAt = now;
        this.status = CrawlStateStatus.COMPLETED;
        this.updatedAt = now;
        this.nextRetryAt = null;
    }

    /**
     * Marks the object as failed with an error.
     *
     * @param error       error message
     * @param nextRetryAt scheduled next retry time
     */
    public void markFailed(String error, OffsetDateTime nextRetryAt) {
        OffsetDateTime now = OffsetDateTime.now();
        this.lastError = error;
        this.updatedAt = now;
        this.status = isRetryExhausted() ? CrawlStateStatus.EXHAUSTED : CrawlStateStatus.FAILED;
        this.nextRetryAt = nextRetryAt;
    }

    /**
     * Updates the record's modification timestamp.
     */
    public void touchUpdatedAt() {
        this.updatedAt = OffsetDateTime.now();
    }

    /**
     * Checks if all retry attempts have been exhausted.
     *
     * @return true if no more retries are allowed
     */
    public boolean isRetryExhausted() {
        return attemptCount > failureAllowance;
    }
}

