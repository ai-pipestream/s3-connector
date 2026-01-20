package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for resolving datasource configuration.
 * <p>
 * Unmanaged connector configuration (no connector-admin dependency).
 */
@ApplicationScoped
public class DatasourceConfigService {

    private static final Logger LOG = Logger.getLogger(DatasourceConfigService.class);

    @Inject
    S3ClientFactory clientFactory;

    private final ConcurrentHashMap<String, DatasourceConfig> configs = new ConcurrentHashMap<>();

    /**
     * Register datasource config with S3 connection details.
     * <p>
     * S3 connection configuration is required for all datasources.
     * This ensures all S3 connections are explicitly configured.
     *
     * @param datasourceId datasource identifier
     * @param apiKey intake API key
     * @param s3Config S3 connection configuration (required)
     * @throws IllegalArgumentException if s3Config is null
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
     * Get datasource configuration including API key.
     *
     * @param datasourceId datasource identifier
     * @return datasource configuration
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
     * Datasource configuration record.
     * <p>
     * Includes both intake API key and S3 connection details.
     * S3ConnectionConfig is required for all datasources.
     */
    public record DatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        public DatasourceConfig {
            if (s3Config == null) {
                throw new IllegalArgumentException("S3ConnectionConfig is required");
            }
        }
    }
}
