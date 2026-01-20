package ai.pipestream.connector.s3.service;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Service interface for resolving KMS (Key Management Service) references to actual credential values.
 * <p>
 * This service provides a unified interface for retrieving secrets stored in external
 * key management systems. It supports various KMS providers including Infisical,
 * AWS KMS, and other secret management services through standardized reference formats.
 * </p>
 *
 * <h2>Supported Reference Formats</h2>
 * <ul>
 *   <li>{@code kms://project/environment/secret-name} - Infisical-style references</li>
 *   <li>{@code arn:aws:kms:region:account:key/key-id} - AWS KMS references</li>
 * </ul>
 *
 * <h2>Implementation Notes</h2>
 * <p>
 * Implementations should handle network failures gracefully and provide appropriate
 * error messages. The {@link #isKmsReference(String)} method provides a default
 * implementation for detecting KMS reference strings.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public interface KmsService {

    /**
     * Resolves a KMS reference to its actual secret value.
     * <p>
     * This method takes a KMS reference string and retrieves the corresponding
     * secret value from the configured KMS provider. If the reference is null
     * or blank, an empty Uni is returned. The method supports various KMS
     * reference formats as documented in the class-level Javadoc.
     * </p>
     *
     * @param kmsRef the KMS reference string to resolve
     * @return a {@link Uni} that completes with the resolved secret value,
     *         or completes empty if the reference is null/blank
     * @throws IllegalArgumentException if the KMS reference format is invalid
     * @throws RuntimeException if the KMS service is unavailable or the reference cannot be resolved
     * @since 1.0.0
     */
    Uni<String> resolveSecret(String kmsRef);

    /**
     * Checks if a string represents a valid KMS reference.
     * <p>
     * This default method provides a basic check for common KMS reference
     * formats. It can be overridden by implementations to support additional
     * reference formats or more sophisticated validation.
     * </p>
     *
     * @param value the string to check for KMS reference format
     * @return {@code true} if the string appears to be a KMS reference,
     *         {@code false} otherwise
     * @since 1.0.0
     */
    default boolean isKmsReference(String value) {
        return value != null && !value.isBlank() &&
               (value.startsWith("kms://") || value.startsWith("arn:aws:kms:"));
    }
}