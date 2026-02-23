package ai.pipestream.connector.s3.target;

/**
 * Crawl run strategy for a target entry.
 */
public enum CrawlTargetMode {
    /**
     * Only perform a full initial crawl.
     */
    INITIAL,

    /**
     * Re-run changes discovered from prior state.
     */
    INCREMENTAL,

    /**
     * Listen and process live source events.
     */
    LIVE,

    /**
     * Run both incremental reconciliation and live events.
     */
    BOTH;
}

