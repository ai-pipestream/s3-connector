package ai.pipestream.connector.s3.state;

import ai.pipestream.connector.s3.v1.S3CrawlPhase;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory state behind {@code StreamCrawlStatus}: one entry per crawl
 * started via {@code StartCrawl}, keyed by its {@code request_id}. The
 * crawl loop increments {@link CrawlStatus#dispatched} as object events
 * are published; completion/failure flips the phase and (on completion)
 * freezes {@code total}.
 *
 * <p>Deliberately memory-only: a connector restart forgets in-flight
 * crawls, and a status subscriber then gets NOT_FOUND — loud, not wrong.
 * Terminal entries are retained so late subscribers still learn the
 * outcome (snapshot semantics), bounded by {@link #MAX_ENTRIES} with
 * oldest-terminal eviction.
 */
@ApplicationScoped
public class CrawlStatusRegistry {

    private static final Logger LOG = Logger.getLogger(CrawlStatusRegistry.class);

    /** Cap on tracked crawls; beyond it the oldest TERMINAL entries are evicted. */
    private static final int MAX_ENTRIES = 500;

    private final Map<String, CrawlStatus> byRequestId = new ConcurrentHashMap<>();

    /** Live view of one crawl. Fields are written by the crawl loop, read by status streams. */
    public static final class CrawlStatus {
        private final String requestId;
        private final AtomicLong dispatched = new AtomicLong();
        private volatile S3CrawlPhase phase = S3CrawlPhase.S3_CRAWL_PHASE_LISTING;
        private volatile long total = -1;
        private volatile String error;
        private volatile long lastUpdatedEpochMs = System.currentTimeMillis();

        private CrawlStatus(String requestId) {
            this.requestId = requestId;
        }

        public String requestId() {
            return requestId;
        }

        public long dispatched() {
            return dispatched.get();
        }

        public S3CrawlPhase phase() {
            return phase;
        }

        /** Final object count; -1 until the crawl COMPLETED. */
        public long total() {
            return total;
        }

        public String error() {
            return error;
        }

        public boolean isTerminal() {
            return phase == S3CrawlPhase.S3_CRAWL_PHASE_COMPLETED
                    || phase == S3CrawlPhase.S3_CRAWL_PHASE_FAILED;
        }
    }

    /** Start tracking a crawl. Idempotent on requestId (re-trigger reuses the entry). */
    public CrawlStatus register(String requestId) {
        evictIfNeeded();
        return byRequestId.computeIfAbsent(requestId, CrawlStatus::new);
    }

    /** One object's crawl event was published. No-op for untracked/blank ids. */
    public void markDispatched(String requestId) {
        if (requestId == null || requestId.isEmpty()) {
            return;
        }
        CrawlStatus status = byRequestId.get(requestId);
        if (status != null) {
            status.dispatched.incrementAndGet();
            status.lastUpdatedEpochMs = System.currentTimeMillis();
        }
    }

    /** The crawl listed and published everything; total freezes at the dispatched count. */
    public void complete(String requestId) {
        CrawlStatus status = byRequestId.get(requestId);
        if (status == null) {
            return;
        }
        status.total = status.dispatched.get();
        status.phase = S3CrawlPhase.S3_CRAWL_PHASE_COMPLETED;
        status.lastUpdatedEpochMs = System.currentTimeMillis();
        LOG.infof("Crawl %s COMPLETED: %d object(s) dispatched", requestId, status.total);
    }

    /** The crawl aborted. dispatched() tells how far it got. */
    public void fail(String requestId, String error) {
        CrawlStatus status = byRequestId.get(requestId);
        if (status == null) {
            return;
        }
        status.error = error;
        status.phase = S3CrawlPhase.S3_CRAWL_PHASE_FAILED;
        status.lastUpdatedEpochMs = System.currentTimeMillis();
        LOG.warnf("Crawl %s FAILED after %d object(s): %s", requestId, status.dispatched(), error);
    }

    /** @return the crawl's live status, or null when unknown (never started, or evicted/restart). */
    public CrawlStatus get(String requestId) {
        return byRequestId.get(requestId);
    }

    /**
     * Keep the map bounded: when full, drop the oldest TERMINAL entries.
     * Active (LISTING) crawls are never evicted — if 500 crawls are
     * genuinely live at once, the map grows past the cap rather than
     * losing track of running work.
     */
    private void evictIfNeeded() {
        if (byRequestId.size() < MAX_ENTRIES) {
            return;
        }
        byRequestId.values().stream()
                .filter(CrawlStatus::isTerminal)
                .sorted(Comparator.comparingLong(s -> s.lastUpdatedEpochMs))
                .limit(Math.max(1, byRequestId.size() - MAX_ENTRIES + 1))
                .forEach(s -> byRequestId.remove(s.requestId()));
    }
}
