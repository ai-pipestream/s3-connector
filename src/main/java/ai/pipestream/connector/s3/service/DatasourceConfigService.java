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
     * Register datasource config from a trigger request.
     * <p>
     * S3 connection details can be provided for datasource-specific S3 access.
     * If not provided, defaults to anonymous credentials (for public buckets).
     *
     * @param datasourceId datasource identifier
     * @param apiKey intake API key
     * @param s3Config optional S3 connection configuration
     */
    public void registerDatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("Datasource ID is required");
        }
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("API key is required");
        }

        // Default to anonymous config if null
        if (s3Config == null) {
            s3Config = S3ConnectionConfig.newBuilder()
                .setCredentialsType("anonymous")
                .setRegion("us-east-1")
                .build();
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
     * Register datasource config with minimal parameters (for backward compatibility).
     * Uses defaults: anonymous credentials, us-east-1 region.
     *
     * @param datasourceId datasource identifier
     * @param apiKey intake API key
     */
    public void registerDatasourceConfig(String datasourceId, String apiKey) {
        registerDatasourceConfig(datasourceId, apiKey, null);
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
     * Includes both intake API key and S3 connection details for multi-connection support.
     */
    public record DatasourceConfig(String datasourceId, String apiKey, S3ConnectionConfig s3Config) {
        /**
         * Constructor for backward compatibility with minimal config.
         */
        public DatasourceConfig(String datasourceId, String apiKey) {
            this(datasourceId, apiKey, null);
        }
    }
}
