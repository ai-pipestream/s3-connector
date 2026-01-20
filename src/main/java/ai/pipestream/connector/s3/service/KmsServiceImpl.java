package ai.pipestream.connector.s3.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic implementation of {@link KmsService} for development and testing environments.
 * <p>
 * This implementation provides a simple in-memory secret store suitable for development
 * and testing scenarios. In production environments, this should be replaced with
 * integrations to actual Key Management Services such as:
 * </p>
 * <ul>
 *   <li>AWS Key Management Service (KMS)</li>
 *   <li>Infisical</li>
 *   <li>HashiCorp Vault</li>
 *   <li>Azure Key Vault</li>
 *   <li>Google Cloud KMS</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>
 * The service can be configured through the following properties:
 * </p>
 * <ul>
 *   <li>{@code kms.enabled} - Enable/disable KMS resolution (default: true)</li>
 *   <li>{@code kms.dev.access-key} - Default S3 access key for development</li>
 *   <li>{@code kms.dev.secret-key} - Default S3 secret key for development</li>
 *   <li>{@code kms.dev.api-key} - Default API key for development</li>
 * </ul>
 *
 * <h2>Security Note</h2>
 * <p>
 * This implementation stores secrets in memory and is not suitable for production use.
 * All secrets are logged at debug level, which should be disabled in production.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class KmsServiceImpl implements KmsService {

    private static final Logger LOG = Logger.getLogger(KmsServiceImpl.class);

    // In-memory store for development/testing - in production this would be external KMS
    private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();

    @Inject
    @ConfigProperty(name = "kms.enabled", defaultValue = "true")
    boolean kmsEnabled;

    @Inject
    @ConfigProperty(name = "kms.dev.access-key", defaultValue = "minioadmin")
    String devAccessKey;

    @Inject
    @ConfigProperty(name = "kms.dev.secret-key", defaultValue = "minioadmin")
    String devSecretKey;

    @Inject
    @ConfigProperty(name = "kms.dev.api-key", defaultValue = "test-api-key")
    String devApiKey;

    /**
     * Creates a new KmsServiceImpl with default development secrets.
     * <p>
     * Initializes the in-memory secret store with common development
     * references that can be overridden via configuration properties.
     * </p>
     */
    public KmsServiceImpl() {
        // Initialize with some default development secrets
        secrets.put("kms://dev/s3/access-key", "minioadmin");
        secrets.put("kms://dev/s3/secret-key", "minioadmin");
        secrets.put("kms://dev/api/key", "test-api-key");
    }

    @Override
    public Uni<String> resolveSecret(String kmsRef) {
        if (kmsRef == null || kmsRef.isBlank()) {
            return Uni.createFrom().item("");
        }

        if (!kmsEnabled) {
            LOG.warnf("KMS is disabled, cannot resolve reference: %s", kmsRef);
            return Uni.createFrom().failure(new IllegalStateException("KMS service is disabled"));
        }

        LOG.debugf("Resolving KMS reference: %s", kmsRef);

        // Check if it's a known reference
        String resolved = secrets.get(kmsRef);

        if (resolved != null) {
            LOG.debugf("Resolved KMS reference %s to value (length: %d)", kmsRef, resolved.length());
            return Uni.createFrom().item(resolved);
        }

        // Handle common development references
        if ("kms://dev/s3/access-key".equals(kmsRef)) {
            return Uni.createFrom().item(devAccessKey);
        }
        if ("kms://dev/s3/secret-key".equals(kmsRef)) {
            return Uni.createFrom().item(devSecretKey);
        }
        if ("kms://dev/api/key".equals(kmsRef)) {
            return Uni.createFrom().item(devApiKey);
        }

        LOG.warnf("Unknown KMS reference: %s", kmsRef);
        return Uni.createFrom().failure(new IllegalArgumentException("Unknown KMS reference: " + kmsRef));
    }

    /**
     * Stores a secret in the in-memory store for development and testing purposes.
     * <p>
     * This method allows programmatic storage of secrets for testing scenarios.
     * In production environments, secrets should be managed externally through
     * proper KMS systems and not stored in application memory.
     * </p>
     *
     * @param kmsRef the KMS reference string to associate with the secret
     * @param value the secret value to store
     * @since 1.0.0
     */
    public void storeSecret(String kmsRef, String value) {
        if (kmsRef != null && value != null) {
            secrets.put(kmsRef, value);
            LOG.debugf("Stored secret for KMS reference: %s", kmsRef);
        }
    }
}