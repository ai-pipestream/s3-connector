package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

/**
 * Startup service to trigger initial S3 bucket crawl.
 * <p>
 * Performs initial crawl on application startup if enabled in configuration.
 */
@ApplicationScoped
public class S3ConnectorStartupService {

    private static final Logger LOG = Logger.getLogger(S3ConnectorStartupService.class);

    @Inject
    S3CrawlService crawlService;

    @Inject
    S3ConnectorConfig config;

    /**
     * Trigger initial crawl on startup if enabled.
     * <p>
     * Non-blocking - does not delay application startup.
     */
    void onStartup(@Observes StartupEvent event) {
        if (!config.initialCrawl().enabled()) {
            LOG.info("Initial crawl is disabled - skipping startup crawl");
            return;
        }

        LOG.info("Starting non-blocking initial S3 crawl on startup...");
        
        // TODO: Get datasource ID and bucket from configuration or connector-admin
        // For MVP, use config defaults
        String datasourceId = "default-datasource"; // TODO: Get from connector-admin
        String bucket = config.defaultBucket();
        String prefix = config.initialCrawl().prefix();

        crawlService.crawlBucket(datasourceId, bucket, prefix)
            .thenRun(() -> {
                LOG.infof("Initial crawl complete: datasourceId=%s, bucket=%s, prefix=%s", 
                    datasourceId, bucket, prefix);
            })
            .exceptionally(error -> {
                LOG.errorf(error, "Initial crawl failed: datasourceId=%s, bucket=%s", datasourceId, bucket);
                return null;
            });
    }
}
