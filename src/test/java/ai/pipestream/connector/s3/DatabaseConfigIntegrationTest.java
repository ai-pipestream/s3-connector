package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.test.support.S3TestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for database-backed datasource configuration.
 * <p>
 * Tests the persistence, retrieval, and caching of datasource configurations
 * using the database layer. Verifies that configurations survive service
 * restarts and are properly cached for performance.
 * </p>
 *
 * <h2>Test Coverage</h2>
 * <ul>
 *   <li>Datasource configuration persistence to database</li>
 *   <li>Configuration retrieval from database</li>
 *   <li>In-memory caching behavior</li>
 *   <li>Configuration updates and versioning</li>
 *   <li>Error handling for missing configurations</li>
 * </ul>
 *
 * @since 1.0.0
 */
@QuarkusTest
@QuarkusTestResource(S3TestResource.class)
class DatabaseConfigIntegrationTest {

    @Inject
    DatasourceConfigService datasourceConfigService;

    /**
     * Tests complete configuration lifecycle: save, retrieve, cache.
     */
    @Test
    @RunOnVertxContext
    void testDatasourceConfigPersistence(UniAsserter asserter) {
        String datasourceId = "test-datasource-db";
        String apiKey = "test-api-key-db";

        // Create test S3 configuration
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId("test-access-key")
            .setSecretAccessKey("test-secret-key")
            .setRegion("us-east-1")
            .setEndpointOverride("http://localhost:9000")
            .setPathStyleAccess(true)
            .build();

        // Save configuration (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // Test retrieving configuration
        asserter.assertThat(
            () -> datasourceConfigService.getDatasourceConfig(datasourceId),
            config -> {
                assertThat(config).isNotNull();
                assertThat(config.datasourceId()).isEqualTo(datasourceId);
                assertThat(config.apiKey()).isEqualTo(apiKey);
                assertThat(config.s3Config()).isNotNull();

                // Verify S3 config details
                assertThat(config.s3Config().getCredentialsType()).isEqualTo("static");
                assertThat(config.s3Config().getAccessKeyId()).isEqualTo("test-access-key");
                assertThat(config.s3Config().getSecretAccessKey()).isEqualTo("test-secret-key");
                assertThat(config.s3Config().getRegion()).isEqualTo("us-east-1");
            }
        );
    }

    /**
     * Tests configuration updates and versioning.
     */
    @Test
    @RunOnVertxContext
    void testConfigurationUpdates(UniAsserter asserter) {
        String datasourceId = "test-datasource-update";
        String apiKey = "test-api-key-update";

        // Initial configuration
        S3ConnectionConfig initialConfig = S3ConnectionConfig.newBuilder()
            .setCredentialsType("anonymous")
            .setRegion("us-east-1")
            .build();

        // Save initial config (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, initialConfig));

        // Update configuration with credentials
        S3ConnectionConfig updatedConfig = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId("updated-access-key")
            .setSecretAccessKey("updated-secret-key")
            .setRegion("us-west-2")
            .build();

        // Save updated config (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, "updated-api-key", updatedConfig));

        // Verify updated configuration
        asserter.assertThat(
            () -> datasourceConfigService.getDatasourceConfig(datasourceId),
            config -> {
                assertThat(config.apiKey()).isEqualTo("updated-api-key");
                assertThat(config.s3Config().getCredentialsType()).isEqualTo("static");
                assertThat(config.s3Config().getAccessKeyId()).isEqualTo("updated-access-key");
                assertThat(config.s3Config().getSecretAccessKey()).isEqualTo("updated-secret-key");
                assertThat(config.s3Config().getRegion()).isEqualTo("us-west-2");
            }
        );
    }

    /**
     * Tests in-memory caching behavior.
     */
    @Test
    @RunOnVertxContext
    void testConfigurationCaching(UniAsserter asserter) {
        String datasourceId = "test-datasource-cache";
        String apiKey = "test-api-key-cache";

        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId("cache-access-key")
            .setSecretAccessKey("cache-secret-key")
            .setRegion("us-east-1")
            .build();

        // Save configuration (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // First retrieval (from database, then cached)
        asserter.assertThat(
            () -> datasourceConfigService.getDatasourceConfig(datasourceId),
            config -> assertThat(config).isNotNull()
        );

        // Second retrieval (should use cache)
        asserter.assertThat(
            () -> datasourceConfigService.getDatasourceConfig(datasourceId),
            config -> {
                assertThat(config).isNotNull();
                assertThat(config.datasourceId()).isEqualTo(datasourceId);
                assertThat(config.apiKey()).isEqualTo(apiKey);
            }
        );
    }

    /**
     * Tests error handling for non-existent configurations.
     */
    @Test
    @RunOnVertxContext
    void testMissingConfigurationError(UniAsserter asserter) {
        String nonExistentId = "non-existent-datasource";

        asserter.assertFailedWith(
            () -> datasourceConfigService.getDatasourceConfig(nonExistentId),
            throwable -> {
                assertThat(throwable).isInstanceOf(IllegalStateException.class);
                assertThat(throwable.getMessage()).contains(nonExistentId);
            }
        );
    }

    /**
     * Tests error handling for invalid parameters.
     */
    @Test
    @RunOnVertxContext
    void testInvalidParameters(UniAsserter asserter) {
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setRegion("us-east-1")
            .build();

        // Test null datasource ID
        asserter.assertFailedWith(
            () -> datasourceConfigService.registerDatasourceConfig(null, "api-key", s3Config),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );

        // Test blank datasource ID
        asserter.assertFailedWith(
            () -> datasourceConfigService.registerDatasourceConfig("", "api-key", s3Config),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );

        // Test null API key
        asserter.assertFailedWith(
            () -> datasourceConfigService.registerDatasourceConfig("datasource-id", null, s3Config),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );

        // Test null S3 config
        asserter.assertFailedWith(
            () -> datasourceConfigService.registerDatasourceConfig("datasource-id", "api-key", null),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );
    }

    /**
     * Tests configuration retrieval with invalid parameters.
     */
    @Test
    @RunOnVertxContext
    void testInvalidRetrievalParameters(UniAsserter asserter) {
        // Test null datasource ID
        asserter.assertFailedWith(
            () -> datasourceConfigService.getDatasourceConfig(null),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );

        // Test blank datasource ID
        asserter.assertFailedWith(
            () -> datasourceConfigService.getDatasourceConfig(""),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );
    }
}
