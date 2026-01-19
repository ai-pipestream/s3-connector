package ai.pipestream.connector.s3.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/**
 * S3 connector configuration.
 */
@ConfigMapping(prefix = "s3.connector")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface S3ConnectorConfig {

    /**
     * Crawl mode: initial-crawl, event-driven, or both.
     */
    @WithDefault("initial-crawl")
    String crawlMode();

    /**
     * Initial crawl configuration.
     */
    InitialCrawlConfig initialCrawl();

    /**
     * Event-driven mode configuration.
     */
    EventDrivenConfig eventDriven();

    /**
     * Connector intake service configuration.
     */
    IntakeConfig intake();

    interface InitialCrawlConfig {
        @WithDefault("true")
        boolean enabled();

        java.util.Optional<String> prefix();

        @WithDefault("1000")
        int maxKeysPerRequest();
    }

    interface EventDrivenConfig {
        @WithDefault("false")
        boolean enabled();
    }

    interface IntakeConfig {
        @WithDefault("http://localhost:38103")
        String baseUrl();

        @WithDefault("/uploads/raw")
        String rawPath();

        @WithDefault("PT5M")
        Duration requestTimeout();
    }
}
