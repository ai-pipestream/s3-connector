package ai.pipestream.connector.s3.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
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

    private final ConcurrentHashMap<String, DatasourceConfig> configs = new ConcurrentHashMap<>();

    /**
     * Register datasource config from a trigger request.
     *
     * @param datasourceId datasource identifier
     * @param apiKey intake API key
     */
    public void registerDatasourceConfig(String datasourceId, String apiKey) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("Datasource ID is required");
        }
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("API key is required");
        }

        configs.put(datasourceId, new DatasourceConfig(datasourceId, apiKey));
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
     */
    public record DatasourceConfig(String datasourceId, String apiKey) {}
}
