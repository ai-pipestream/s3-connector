package ai.pipestream.connector.s3.service;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
 * Factory for creating and managing {@link S3AsyncClient} instances per datasource.
 * <p>
 * This factory enables the S3 connector to support multiple datasources with different
 * S3 configurations simultaneously. Each datasource can have its own credentials,
 * region, endpoint, and connection settings. The factory provides both cached clients
 * for production use and temporary clients for testing.
 * </p>
 *
 * <h2>Client Caching</h2>
 * <p>
 * Production clients (created via {@link #getOrCreateClient(String, S3ConnectionConfig)})
 * are cached by datasource ID to improve performance and reduce connection overhead.
 * The cache is invalidated when datasource configurations change.
 * </p>
 *
 * <h2>Credential Resolution</h2>
 * <p>
 * The factory supports multiple credential sources:
 * </p>
 * <ul>
 *   <li>Anonymous credentials for public buckets</li>
 *   <li>Static credentials provided directly</li>
 *   <li>KMS-resolved credentials for secure secret management</li>
 * </ul>
 *
 * <h2>Resource Management</h2>
 * <p>
 * Clients should be properly closed to release network resources. The factory
 * provides methods to close individual clients or all cached clients during shutdown.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class S3ClientFactory {

    /**
     * Default constructor for CDI injection.
     */
    public S3ClientFactory() {
    }

    private static final Logger LOG = Logger.getLogger(S3ClientFactory.class);

    @Inject
    KmsService kmsService;

    // Cache clients per datasource to avoid recreating them
    private final Map<String, S3AsyncClient> datasourceClientCache = new ConcurrentHashMap<>();

    /**
     * Gets or creates a cached {@link S3AsyncClient} for the specified datasource.
     * <p>
     * This method returns a cached client if one exists for the datasource,
     * otherwise creates a new client and caches it for future use. The client
     * is configured based on the provided S3 connection configuration.
     * </p>
     *
     * @param datasourceId the unique identifier for the datasource
     * @param config the S3 connection configuration (required, cannot be null)
     * @return a {@link Uni} that completes with the configured {@link S3AsyncClient}
     * @throws IllegalArgumentException if config is null
     * @since 1.0.0
     */
    public Uni<S3AsyncClient> getOrCreateClient(String datasourceId, S3ConnectionConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("S3ConnectionConfig is required for datasource: " + datasourceId);
        }

        S3AsyncClient cached = datasourceClientCache.get(datasourceId);
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }

        return createClientAsync(config, "datasource-" + datasourceId)
            .map(client -> {
                datasourceClientCache.put(datasourceId, client);
                return client;
            });
    }

    /**
     * Creates a temporary {@link S3AsyncClient} for testing purposes.
     * <p>
     * This method creates a new client that is not cached and should be
     * explicitly closed after testing is complete. Test clients are useful
     * for validation and testing without affecting the production client cache.
     * </p>
     *
     * @param config the S3 connection configuration for testing (required, cannot be null)
     * @return a {@link Uni} that completes with a temporary {@link S3AsyncClient} for testing
     * @throws IllegalArgumentException if config is null
     * @since 1.0.0
     */
    public Uni<S3AsyncClient> createTestClient(S3ConnectionConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("S3ConnectionConfig is required for test client");
        }
        return createClientAsync(config, "test-client");
    }

    /**
     * Create a new S3AsyncClient based on connection configuration.
     * <p>
     * S3ConnectionConfig is guaranteed to be non-null when this method is called.
     *
     * @param config S3 connection configuration (guaranteed non-null)
     * @param clientName name for logging purposes
     * @return Uni containing configured S3AsyncClient
     */
    private Uni<S3AsyncClient> createClientAsync(S3ConnectionConfig config, String clientName) {
        LOG.infof("Creating S3AsyncClient for %s", clientName);

        // Region (default to us-east-1 if not specified)
        final String region = config.getRegion() != null && !config.getRegion().isBlank()
            ? config.getRegion() : "us-east-1";

        // Resolve credentials - this may involve KMS lookups
        return resolveCredentials(config, clientName)
            .map(credentialsProvider -> {
                var builder = S3AsyncClient.builder();
                builder.region(Region.of(region));
                builder.credentialsProvider(credentialsProvider);

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
            });
    }

    /**
     * Resolve credentials from the connection config, supporting both static and KMS-based credentials.
     *
     * @param config S3 connection configuration
     * @param clientName name for logging
     * @return Uni containing the credentials provider
     */
    private Uni<software.amazon.awssdk.auth.credentials.AwsCredentialsProvider> resolveCredentials(
            S3ConnectionConfig config, String clientName) {

        String credentialsType = config.getCredentialsType();

        if (credentialsType != null && "anonymous".equals(credentialsType)) {
            // Anonymous credentials for public buckets
            LOG.debugf("Using anonymous credentials for %s", clientName);
            return Uni.createFrom().item(AnonymousCredentialsProvider.create());
        }

        if (credentialsType != null && "static".equals(credentialsType)) {
            // Try static credentials first (direct values)
            String accessKey = config.getAccessKeyId();
            String secretKey = config.getSecretAccessKey();

            if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                LOG.debugf("Using static credentials for %s", clientName);
                return Uni.createFrom().item(StaticCredentialsProvider.create(credentials));
            }

            // Fall back to KMS references if direct credentials not provided
            String kmsAccessKeyRef = config.getKmsAccessKeyRef();
            String kmsSecretKeyRef = config.getKmsSecretKeyRef();

            if (kmsAccessKeyRef != null && !kmsAccessKeyRef.isBlank() &&
                kmsSecretKeyRef != null && !kmsSecretKeyRef.isBlank()) {

                LOG.debugf("Resolving KMS credentials for %s", clientName);
                return kmsService.resolveSecret(kmsAccessKeyRef)
                    .flatMap(resolvedAccessKey ->
                        kmsService.resolveSecret(kmsSecretKeyRef)
                            .map(resolvedSecretKey -> {
                                AwsBasicCredentials credentials = AwsBasicCredentials.create(resolvedAccessKey, resolvedSecretKey);
                                LOG.debugf("Using KMS-resolved credentials for %s", clientName);
                                return StaticCredentialsProvider.create(credentials);
                            })
                    );
            }
        }

        // Default to anonymous if no credentials specified
        LOG.debugf("No credentials specified, using anonymous for %s", clientName);
        return Uni.createFrom().item(AnonymousCredentialsProvider.create());
    }

    /**
     * Closes and removes a cached client for the specified datasource.
     * <p>
     * This method should be called when a datasource's configuration changes
     * to ensure that subsequent client requests use the updated configuration.
     * The client is properly closed to release network resources.
     * </p>
     *
     * @param datasourceId the datasource identifier whose client should be closed
     * @since 1.0.0
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
     * Closes all cached clients and clears the client cache.
     * <p>
     * This method is typically called during application shutdown to ensure
     * all network resources are properly released. It safely handles any
     * exceptions that occur during client closure.
     * </p>
     *
     * @since 1.0.0
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