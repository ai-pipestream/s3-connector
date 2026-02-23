package ai.pipestream.connector.s3.state;

/**
 * Source of crawl work for a given object.
 */
public enum CrawlSource {
    /**
     * Crawl was triggered by the initial bucket scan.
     */
    INITIAL,

    /**
     * Crawl was triggered by incremental reconciliation logic.
     */
    INCREMENTAL,

    /**
     * Crawl was triggered by a live source event.
     */
    LIVE
}

