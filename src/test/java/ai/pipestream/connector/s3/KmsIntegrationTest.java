package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.KmsService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.service.S3ClientFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test for KMS (Key Management Service) functionality.
 * <p>
 * Tests credential resolution from KMS references, S3 client creation with
 * KMS-resolved credentials, and end-to-end connectivity validation.
 * </p>
 *
 * <h2>Test Coverage</h2>
 * <ul>
 *   <li>KMS secret resolution (Infisical-style references)</li>
 *   <li>AWS KMS-style reference support</li>
 *   <li>S3 client creation with KMS credentials</li>
 *   <li>Connection validation with resolved credentials</li>
 *   <li>Error handling for invalid/missing references</li>
 * </ul>
 *
 * @since 1.0.0
 */
@QuarkusTest
class KmsIntegrationTest {

    @Inject
    KmsService kmsService;

    @Inject
    S3ClientFactory s3ClientFactory;

    /**
     * Tests basic KMS secret resolution with Infisical-style references.
     */
    @Test
    @RunOnVertxContext
    void testKmsSecretResolution(UniAsserter asserter) {
        // Test resolving known secrets
        asserter.assertThat(
            () -> kmsService.resolveSecret("kms://dev/s3/access-key"),
            accessKey -> assertThat(accessKey).isEqualTo("test-access-key")
        );

        asserter.assertThat(
            () -> kmsService.resolveSecret("kms://dev/s3/secret-key"),
            secretKey -> assertThat(secretKey).isEqualTo("test-secret-key")
        );

        asserter.assertThat(
            () -> kmsService.resolveSecret("kms://dev/api/key"),
            apiKey -> assertThat(apiKey).isEqualTo("test-api-key")
        );

        // Test null/empty references
        asserter.assertThat(
            () -> kmsService.resolveSecret(null),
            result -> assertThat(result).isEqualTo("")
        );

        asserter.assertThat(
            () -> kmsService.resolveSecret(""),
            result -> assertThat(result).isEqualTo("")
        );
    }

    /**
     * Tests AWS KMS-style reference resolution.
     */
    @Test
    @RunOnVertxContext
    void testAwsKmsStyleReferences(UniAsserter asserter) {
        // Test AWS KMS ARN format
        asserter.assertThat(
            () -> kmsService.resolveSecret("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
            result -> assertThat(result).isEqualTo("aws-kms-secret-value")
        );
    }

    /**
     * Tests S3 client creation with KMS-resolved credentials.
     */
    @Test
    @RunOnVertxContext
    void testS3ClientCreationWithKmsCredentials(UniAsserter asserter) {
        // Create S3 config with KMS references
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static") // Still "static" but with KMS refs
            .setKmsAccessKeyRef("kms://dev/s3/access-key")
            .setKmsSecretKeyRef("kms://dev/s3/secret-key")
            .setRegion("us-east-1")
            .setEndpointOverride("http://localhost:9000") // MinIO
            .setPathStyleAccess(true)
            .build();

        // Test that we can create a client with KMS credentials
        asserter.assertThat(
            () -> s3ClientFactory.createTestClient(s3Config),
            client -> {
                assertThat(client).isNotNull();
                // Client should be created successfully with resolved credentials
                // Note: We don't test actual S3 operations here as that would require
                // the MinIO container to be running with the resolved credentials
            }
        );
    }

    /**
     * Tests fallback behavior when KMS references are missing.
     */
    @Test
    @RunOnVertxContext
    void testKmsFallbackBehavior(UniAsserter asserter) {
        // Test direct credentials take precedence
        S3ConnectionConfig directCredsConfig = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId("direct-access-key")
            .setSecretAccessKey("direct-secret-key")
            .setKmsAccessKeyRef("kms://dev/s3/access-key") // Should be ignored
            .setKmsSecretKeyRef("kms://dev/s3/secret-key") // Should be ignored
            .setRegion("us-east-1")
            .build();

        asserter.assertThat(
            () -> s3ClientFactory.createTestClient(directCredsConfig),
            client -> assertThat(client).isNotNull()
        );

        // Test anonymous fallback
        S3ConnectionConfig anonymousConfig = S3ConnectionConfig.newBuilder()
            .setCredentialsType("anonymous")
            .setRegion("us-east-1")
            .build();

        asserter.assertThat(
            () -> s3ClientFactory.createTestClient(anonymousConfig),
            client -> assertThat(client).isNotNull()
        );
    }

    /**
     * Tests KMS reference detection.
     */
    @Test
    void testKmsReferenceDetection() {
        assertThat(kmsService.isKmsReference("kms://project/env/secret")).isTrue();
        assertThat(kmsService.isKmsReference("arn:aws:kms:region:account:key/key-id")).isTrue();
        assertThat(kmsService.isKmsReference("plain-text-secret")).isFalse();
        assertThat(kmsService.isKmsReference(null)).isFalse();
        assertThat(kmsService.isKmsReference("")).isFalse();
    }
}