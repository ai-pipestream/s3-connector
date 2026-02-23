package ai.pipestream.connector.s3.state;

/**
 * Lifecycle status for a tracked crawl item.
 */
public enum CrawlStateStatus {
    /**
     * Item has been discovered and is waiting to be emitted to the intake pipeline.
     */
    PENDING,

    /**
     * Item is currently being emitted/processed.
     */
    IN_PROGRESS,

    /**
     * Item was successfully emitted and acknowledged by the pipeline.
     */
    COMPLETED,

    /**
     * Item failed and may be retried according to backoff policy.
     */
    FAILED,

    /**
     * Item has exceeded max attempts or was manually marked non-retriable.
     */
    EXHAUSTED;
}

