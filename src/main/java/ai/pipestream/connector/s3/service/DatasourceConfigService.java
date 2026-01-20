package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing datasource configurations in unmanaged S3 connector deployments.
 * <p>
 * This service handles the registration and retrieval of datasource configurations
 * for S3 connectors that operate independently of the connector-admin service.
 * It maintains an in-memory registry of datasource settings including API keys
 * and S3 connection parameters.
 * </p>
 *
 * <h2>Configuration Management</h2>
 * <p>
 * Configurations are stored in memory and must be registered before use.
 * Changes to configuration automatically invalidate cached S3 clients to
 * ensure connection parameters are refreshed.
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

    private final ConcurrentHashMap<String, DatasourceConfig> configs = new ConcurrentHashMap<>();

    /**
     * Registers or updates a datasource configuration.
     * <p>
     * This method stores the datasource configuration in the in-memory registry.
     * If the configuration for the datasource already exists and has changed,
     * any cached S3 client for that datasource is automatically invalidated
     * to ensure connection parameters are refreshed.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param apiKey the API key for accessing the connector-intake-service
     * @param s3Config the S3 connection configuration (required, cannot be null)
     * @throws IllegalArgumentException if any parameter is null, blank, or invalid
     * @since 1.0.0
     */
    public void registerDatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("Datasource ID is required");
        }
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("API key is required");
        }
        if (s3Config == null) {
            throw new IllegalArgumentException("S3ConnectionConfig is required for datasource: " + datasourceId);
        }

        // Check if config already exists and has changed
        DatasourceConfig existing = configs.get(datasourceId);
        DatasourceConfig newConfig = new DatasourceConfig(datasourceId, apiKey, s3Config);

        if (existing != null && !existing.equals(newConfig)) {
            // Config changed, invalidate cached client
            LOG.infof("Datasource config changed for %s, invalidating cached S3 client", datasourceId);
            clientFactory.closeClient(datasourceId);
        }

        configs.put(datasourceId, newConfig);
        LOG.debugf("Registered datasource config: datasourceId=%s", datasourceId);
    }

    /**
     * Retrieves the configuration for a registered datasource.
     * <p>
     * Returns the complete datasource configuration including the API key
     * and S3 connection parameters. The datasource must have been previously
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
        DatasourceConfig config = configs.get(datasourceId);
        if (config == null) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Datasource config not registered for datasourceId=" + datasourceId));
        }
        return Uni.createFrom().item(config);
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
