package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.entity.DatasourceConfigEntity;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing datasource configurations in S3 connector deployments.
 * <p>
 * This service handles the registration and retrieval of datasource configurations
 * for S3 connectors. It persists configurations to the database for durability
 * across service restarts and enables sharing configurations between multiple
 * connector instances.
 * </p>
 *
 * <h2>Configuration Management</h2>
 * <p>
 * Configurations are stored in the database and cached in memory for performance.
 * Changes to configuration automatically invalidate cached S3 clients to
 * ensure connection parameters are refreshed.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This service is thread-safe and can be used concurrently from multiple threads.
 * Database operations are handled reactively to avoid blocking.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class DatasourceConfigService {

    /**
     * Default constructor for CDI injection.
     */
    public DatasourceConfigService() {
    }

    private static final Logger LOG = Logger.getLogger(DatasourceConfigService.class);

    @Inject
    S3ClientFactory clientFactory;

    @Inject
    ObjectMapper objectMapper;

    // In-memory cache for performance
    private final ConcurrentHashMap<String, DatasourceConfig> configCache = new ConcurrentHashMap<>();

    /**
     * Registers or updates a datasource configuration.
     * <p>
     * This method persists the datasource configuration to the database and updates
     * the in-memory cache. If the configuration for the datasource already exists
     * and has changed, any cached S3 client for that datasource is automatically
     * invalidated to ensure connection parameters are refreshed.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param apiKey the API key for accessing the connector-intake-service
     * @param s3Config the S3 connection configuration (required, cannot be null)
     * @return a Uni that completes when the configuration has been saved
     * @throws IllegalArgumentException if any parameter is null, blank, or invalid
     * @since 1.0.0
     */
    public Uni<Void> registerDatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("Datasource ID is required");
        }
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("API key is required");
        }
        if (s3Config == null) {
            throw new IllegalArgumentException("S3ConnectionConfig is required for datasource: " + datasourceId);
        }

        return Uni.createFrom().item(() -> {
            try {
                String s3ConfigJson = objectMapper.writeValueAsString(s3Config);
                return new Object[]{s3ConfigJson, new DatasourceConfig(datasourceId, apiKey, s3Config)};
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize S3 config", e);
            }
        }).flatMap(tuple -> {
            String s3ConfigJson = (String) tuple[0];
            DatasourceConfig newConfig = (DatasourceConfig) tuple[1];

            // Check if config already exists and has changed
            DatasourceConfig existing = configCache.get(datasourceId);
            if (existing != null && !existing.equals(newConfig)) {
                // Config changed, invalidate cached client
                LOG.infof("Datasource config changed for %s, invalidating cached S3 client", datasourceId);
                clientFactory.closeClient(datasourceId);
            }

            // Save to database
            return DatasourceConfigEntity.<DatasourceConfigEntity>findById(datasourceId)
                .flatMap(existingEntity -> {
                    if (existingEntity != null) {
                        // Update existing
                        existingEntity.updateS3Config(s3ConfigJson);
                        existingEntity.updateApiKey(apiKey);
                        return existingEntity.<DatasourceConfigEntity>persist();
                    } else {
                        // Create new
                        DatasourceConfigEntity newEntity = new DatasourceConfigEntity(datasourceId, apiKey, s3ConfigJson);
                        return newEntity.<DatasourceConfigEntity>persist();
                    }
                })
                .invoke(() -> {
                    // Update cache
                    configCache.put(datasourceId, newConfig);
                    LOG.debugf("Registered datasource config: datasourceId=%s", datasourceId);
                })
                .replaceWithVoid();
        });
    }

    /**
     * Retrieves the configuration for a registered datasource.
     * <p>
     * Returns the complete datasource configuration including the API key
     * and S3 connection parameters. First checks the in-memory cache, then
     * loads from database if not found. The datasource must have been previously
     * registered using {@link #registerDatasourceConfig(String, String, S3ConnectionConfig)}.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @return a {@link Uni} that completes with the {@link DatasourceConfig},
     *         or fails with {@link IllegalArgumentException} if datasourceId is invalid,
     *         or {@link IllegalStateException} if no configuration is registered
     * @since 1.0.0
     */
    public Uni<DatasourceConfig> getDatasourceConfig(String datasourceId) {
        LOG.debugf("Getting datasource config: datasourceId=%s", datasourceId);

        if (datasourceId == null || datasourceId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("Datasource ID is required"));
        }

        // Check cache first
        DatasourceConfig cached = configCache.get(datasourceId);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }

        // Load from database
        return DatasourceConfigEntity.<DatasourceConfigEntity>findById(datasourceId)
            .flatMap(entity -> {
                if (entity == null) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "Datasource config not registered for datasourceId=" + datasourceId));
                }

                try {
                    S3ConnectionConfig s3Config = objectMapper.readValue(entity.s3ConfigJson, S3ConnectionConfig.class);
                    DatasourceConfig config = new DatasourceConfig(datasourceId, entity.apiKey, s3Config);

                    // Cache for future use
                    configCache.put(datasourceId, config);

                    return Uni.createFrom().item(config);
                } catch (JsonProcessingException e) {
                    return Uni.createFrom().failure(new RuntimeException(
                        "Failed to deserialize S3 config for datasourceId=" + datasourceId, e));
                }
            });
    }

    /**
     * Immutable configuration record for a datasource.
     * <p>
     * This record encapsulates all configuration parameters needed for a datasource,
     * including the identifier, API key for intake service access, and S3 connection
     * configuration. The S3 connection config is validated to be non-null during construction.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param apiKey the API key for accessing the connector-intake-service
     * @param s3Config the S3 connection configuration (validated to be non-null)
     * @since 1.0.0
     */
    public record DatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        /**
         * Compact constructor with validation.
         *
         * @throws IllegalArgumentException if s3Config is null
         */
        public DatasourceConfig {
            if (s3Config == null) {
                throw new IllegalArgumentException("S3ConnectionConfig is required");
            }
        }
    }
}
