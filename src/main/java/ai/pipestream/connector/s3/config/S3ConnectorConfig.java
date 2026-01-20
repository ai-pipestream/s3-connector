package ai.pipestream.connector.s3.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/**
 * Configuration interface for the S3 connector service.
 * <p>
 * This interface defines all configuration properties for the S3 connector,
 * including crawl modes, connection settings, and integration endpoints.
 * Configuration is loaded at runtime using Quarkus SmallRye Config.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <ul>
 *   <li>{@code s3.connector.crawl-mode} - Operation mode (default: "initial-crawl")</li>
 *   <li>{@code s3.connector.initial-crawl.*} - Initial crawl settings</li>
 *   <li>{@code s3.connector.event-driven.*} - Event-driven crawl settings</li>
 *   <li>{@code s3.connector.intake.*} - Connector intake service settings</li>
 * </ul>
 *
 * @since 1.0.0
 */
@ConfigMapping(prefix = "s3.connector")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface S3ConnectorConfig {

    /**
     * Gets the crawl mode for the S3 connector.
     * <p>
     * Supported modes:
     * <ul>
     *   <li>{@code "initial-crawl"} - Performs one-time bucket crawling</li>
     *   <li>{@code "event-driven"} - Listens for S3 events (not yet implemented)</li>
     *   <li>{@code "both"} - Combines initial crawl with event-driven mode</li>
     * </ul>
     *
     * @return the crawl mode string, defaults to "initial-crawl"
     */
    @WithDefault("initial-crawl")
    String crawlMode();

    /**
     * Gets the initial crawl configuration.
     * <p>
     * Defines settings for one-time bucket crawling operations.
     *
     * @return configuration for initial crawl operations
     */
    InitialCrawlConfig initialCrawl();

    /**
     * Gets the event-driven crawl configuration.
     * <p>
     * Defines settings for event-driven crawling based on S3 notifications.
     * Currently not implemented but reserved for future use.
     *
     * @return configuration for event-driven crawl operations
     */
    EventDrivenConfig eventDriven();

    /**
     * Gets the connector intake service configuration.
     * <p>
     * Defines connection settings for uploading crawled objects to the
     * connector-intake-service.
     *
     * @return configuration for the intake service connection
     */
    IntakeConfig intake();

    /**
     * Configuration for initial crawl operations.
     * <p>
     * Controls how the S3 connector performs one-time bucket crawling,
     * including filtering and pagination settings.
     */
    interface InitialCrawlConfig {

        /**
         * Checks if initial crawling is enabled.
         *
         * @return {@code true} if initial crawling is enabled, {@code false} otherwise
         */
        @WithDefault("true")
        boolean enabled();

        /**
         * Gets the optional S3 object key prefix filter.
         * <p>
         * When specified, only objects with keys starting with this prefix
         * will be included in the crawl. If empty or null, all objects in
         * the bucket are eligible for crawling.
         *
         * @return optional prefix filter, empty if no prefix filtering should be applied
         */
        java.util.Optional<String> prefix();

        /**
         * Gets the maximum number of S3 objects to retrieve per API request.
         * <p>
         * This controls pagination when listing S3 objects. Larger values
         * may improve performance but increase memory usage and API response times.
         *
         * @return maximum keys per request, defaults to 1000
         */
        @WithDefault("1000")
        int maxKeysPerRequest();
    }

    /**
     * Configuration for event-driven crawl operations.
     * <p>
     * Reserved for future implementation of S3 event notification-based crawling.
     * Currently serves as a placeholder for enabling/disabling this feature.
     */
    interface EventDrivenConfig {

        /**
         * Checks if event-driven crawling is enabled.
         * <p>
         * When enabled, the connector will listen for S3 event notifications
         * instead of performing periodic crawls. This feature is not yet implemented.
         *
         * @return {@code true} if event-driven crawling is enabled, {@code false} otherwise
         */
        @WithDefault("false")
        boolean enabled();
    }

    /**
     * Configuration for the connector intake service connection.
     * <p>
     * Defines HTTP client settings for uploading crawled S3 objects to the
     * connector-intake-service for further processing.
     */
    interface IntakeConfig {

        /**
         * Gets the base URL of the connector intake service.
         * <p>
         * This should be the full base URL including protocol and port,
         * e.g., "https://intake.example.com" or "http://localhost:38103".
         *
         * @return the intake service base URL
         */
        @WithDefault("http://localhost:38103")
        String baseUrl();

        /**
         * Gets the URL path for raw object uploads.
         * <p>
         * This path is appended to the base URL to form the complete
         * upload endpoint URL.
         *
         * @return the raw upload path, defaults to "/uploads/raw"
         */
        @WithDefault("/uploads/raw")
        String rawPath();

        /**
         * Gets the request timeout for intake service uploads.
         * <p>
         * Specifies how long the HTTP client will wait for the intake
         * service to respond to upload requests before timing out.
         *
         * @return the request timeout duration, defaults to 5 minutes
         */
        @WithDefault("PT5M")
        Duration requestTimeout();
    }
}
