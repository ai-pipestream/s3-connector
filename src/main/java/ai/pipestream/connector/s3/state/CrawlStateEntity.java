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

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    public Long id;

    @Column(name = "datasource_id", nullable = false, length = 255)
    public String datasourceId;

    @Column(name = "bucket", nullable = false, length = 512)
    public String bucket;

    @Column(name = "object_key", nullable = false, length = 4096)
    public String objectKey;

    @Column(name = "object_version_id", nullable = false, length = 255)
    public String objectVersionId;

    @Column(name = "object_etag", nullable = true, length = 255)
    public String objectEtag;

    @Column(name = "size_bytes", nullable = false)
    public long sizeBytes;

    @Column(name = "last_modified", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastModified;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    public CrawlStateStatus status = CrawlStateStatus.PENDING;

    @Column(name = "attempt_count", nullable = false)
    public int attemptCount;

    @Column(name = "failure_allowance", nullable = false)
    public int failureAllowance = 1;

    @Enumerated(EnumType.STRING)
    @Column(name = "crawl_source", nullable = false, length = 20)
    public CrawlSource crawlSource = CrawlSource.INCREMENTAL;

    @Column(name = "next_retry_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime nextRetryAt;

    @Column(name = "last_attempt_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime lastAttemptAt;

    @Column(name = "completed_at", columnDefinition = "TIMESTAMP WITH TIME ZONE")
    public OffsetDateTime completedAt;

    @Column(name = "fingerprint", length = 255)
    public String fingerprint;

    @Column(name = "last_error", columnDefinition = "TEXT")
    public String lastError;

    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime updatedAt;

    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    public CrawlStateEntity() {
    }

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

    public void markAttemptStarted() {
        this.attemptCount++;
        this.lastAttemptAt = OffsetDateTime.now();
        this.status = CrawlStateStatus.IN_PROGRESS;
        this.updatedAt = this.lastAttemptAt;
        this.lastError = null;
    }

    public void markCompleted() {
        OffsetDateTime now = OffsetDateTime.now();
        this.completedAt = now;
        this.status = CrawlStateStatus.COMPLETED;
        this.updatedAt = now;
        this.nextRetryAt = null;
    }

    public void markFailed(String error, OffsetDateTime nextRetryAt) {
        OffsetDateTime now = OffsetDateTime.now();
        this.lastError = error;
        this.updatedAt = now;
        this.status = isRetryExhausted() ? CrawlStateStatus.EXHAUSTED : CrawlStateStatus.FAILED;
        this.nextRetryAt = nextRetryAt;
    }

    public void touchUpdatedAt() {
        this.updatedAt = OffsetDateTime.now();
    }

    public boolean isRetryExhausted() {
        return attemptCount > failureAllowance;
    }
}

