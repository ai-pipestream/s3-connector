package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating S3AsyncClient instances per datasource or test configuration.
 * <p>
 * Each datasource can have its own S3 configuration (credentials, region, endpoint, etc.),
 * allowing the connector to support multiple S3 connections simultaneously.
 * <p>
 * Clients are cached by datasource ID for reuse. Test configurations are not cached.
 */
@ApplicationScoped
public class S3ClientFactory {

    private static final Logger LOG = Logger.getLogger(S3ClientFactory.class);

    // Cache clients per datasource to avoid recreating them
    private final Map<String, S3AsyncClient> datasourceClientCache = new ConcurrentHashMap<>();

    /**
     * Get or create an S3AsyncClient for the given datasource.
     * <p>
     * Clients are cached and reused for the same datasource configuration.
     *
     * @param datasourceId datasource identifier
     * @param config S3 connection configuration
     * @return S3AsyncClient configured for this datasource
     */
    public S3AsyncClient getOrCreateClient(String datasourceId, S3ConnectionConfig config) {
        return datasourceClientCache.computeIfAbsent(datasourceId,
            id -> createClient(config, "datasource-" + id));
    }

    /**
     * Create a temporary S3AsyncClient for testing purposes.
     * <p>
     * Test clients are not cached and should be closed after use.
     *
     * @param config S3 connection configuration
     * @return temporary S3AsyncClient for testing
     */
    public S3AsyncClient createTestClient(S3ConnectionConfig config) {
        return createClient(config, "test-client");
    }

    /**
     * Create a new S3AsyncClient based on connection configuration.
     * <p>
     * If config is null, defaults to anonymous credentials with us-east-1 region.
     *
     * @param config S3 connection configuration (null defaults to anonymous)
     * @param clientName name for logging purposes
     * @return configured S3AsyncClient
     */
    private S3AsyncClient createClient(S3ConnectionConfig config, String clientName) {
        LOG.infof("Creating S3AsyncClient for %s", clientName);

        var builder = S3AsyncClient.builder();

        // Default to anonymous config if null
        if (config == null) {
            config = S3ConnectionConfig.newBuilder()
                .setCredentialsType("anonymous")
                .setRegion("us-east-1")
                .build();
        }

        // Region (default to us-east-1 if not specified)
        String region = config.getRegion();
        if (region == null || region.isBlank()) {
            region = "us-east-1";
        }
        builder.region(Region.of(region));

        // Credentials
        String credentialsType = config.getCredentialsType();
        if (credentialsType != null && "anonymous".equals(credentialsType)) {
            // Anonymous credentials for public buckets
            builder.credentialsProvider(AnonymousCredentialsProvider.create());
            LOG.debugf("Using anonymous credentials for %s", clientName);
        } else if (credentialsType != null && "static".equals(credentialsType)) {
            // Static credentials
            String accessKey = config.getAccessKeyId();
            String secretKey = config.getSecretAccessKey();
            if (accessKey != null && secretKey != null) {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
                LOG.debugf("Using static credentials for %s", clientName);
            } else {
                // Fall back to anonymous if no static credentials provided
                builder.credentialsProvider(AnonymousCredentialsProvider.create());
                LOG.debugf("No static credentials provided, using anonymous for %s", clientName);
            }
        } else {
            // Default to anonymous if no credentials type specified
            builder.credentialsProvider(AnonymousCredentialsProvider.create());
            LOG.debugf("No credentials type specified, using anonymous for %s", clientName);
        }

        // Endpoint override (e.g., for MinIO or custom S3-compatible services)
        String endpointOverride = config.getEndpointOverride();
        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
            LOG.debugf("Using custom endpoint for %s: %s", clientName, endpointOverride);
        }

        // Path-style access (needed for MinIO and some S3-compatible services)
        boolean pathStyleAccess = config.getPathStyleAccess();
        builder.serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(pathStyleAccess)
            .build());

        S3AsyncClient client = builder.build();
        LOG.infof("Created S3AsyncClient for %s (region=%s)", clientName, region);

        return client;
    }

    /**
     * Close and remove a cached client for a datasource.
     * Useful when datasource configuration changes.
     *
     * @param datasourceId datasource identifier
     */
    public void closeClient(String datasourceId) {
        S3AsyncClient client = datasourceClientCache.remove(datasourceId);
        if (client != null) {
            try {
                client.close();
                LOG.infof("Closed S3AsyncClient for datasource: %s", datasourceId);
            } catch (Exception e) {
                LOG.warnf(e, "Error closing S3AsyncClient for datasource: %s", datasourceId);
            }
        }
    }

    /**
     * Close all cached clients.
     * Called during application shutdown.
     */
    public void closeAll() {
        LOG.info("Closing all cached S3AsyncClient instances");
        for (Map.Entry<String, S3AsyncClient> entry : datasourceClientCache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.warnf(e, "Error closing S3AsyncClient for datasource: %s", entry.getKey());
            }
        }
        datasourceClientCache.clear();
    }
}