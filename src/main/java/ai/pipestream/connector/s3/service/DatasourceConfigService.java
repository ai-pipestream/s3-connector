package ai.pipestream.connector.s3.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Service for resolving datasource configuration.
 * <p>
 * TODO: Integrate with connector-admin gRPC service to get datasource config and API key.
 * For MVP, returns a mock configuration.
 */
@ApplicationScoped
public class DatasourceConfigService {

    private static final Logger LOG = Logger.getLogger(DatasourceConfigService.class);

    /**
     * Get datasource configuration including API key.
     * <p>
     * TODO: Call connector-admin service via gRPC to get datasource config.
     *
     * @param datasourceId datasource identifier
     * @return datasource configuration
     */
    public Uni<DatasourceConfig> getDatasourceConfig(String datasourceId) {
        LOG.debugf("Getting datasource config: datasourceId=%s", datasourceId);
        
        // TODO: Replace with actual gRPC call to connector-admin
        // For MVP, return mock config
        return Uni.createFrom().item(new DatasourceConfig(
            datasourceId,
            "mock-api-key", // TODO: Get from connector-admin
            "test-bucket"   // TODO: Get from datasource custom config
        ));
    }

    /**
     * Datasource configuration record.
     */
    public record DatasourceConfig(String datasourceId, String apiKey, String bucket) {}
}
