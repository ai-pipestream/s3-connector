package ai.pipestream.connector.s3;

import ai.pipestream.test.support.MinioTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

/**
 * Debug test to verify MinioTestResource config injection.
 */
@QuarkusTest
@QuarkusTestResource(MinioTestResource.class)
class MinioConfigDebugTest {

    @Inject
    Config config;

    @Test
    void debugConfigValues() {
        System.out.println("=== MinIO Config Debug ===");

        System.out.println("quarkus.s3.endpoint-override = " +
            config.getOptionalValue("quarkus.s3.endpoint-override", String.class).orElse("NOT SET"));

        System.out.println("quarkus.s3.aws.credentials.static-provider.access-key-id = " +
            config.getOptionalValue("quarkus.s3.aws.credentials.static-provider.access-key-id", String.class).orElse("NOT SET"));

        System.out.println("quarkus.s3.aws.credentials.static-provider.secret-access-key = " +
            config.getOptionalValue("quarkus.s3.aws.credentials.static-provider.secret-access-key", String.class).orElse("NOT SET"));

        System.out.println("quarkus.s3.aws.region = " +
            config.getOptionalValue("quarkus.s3.aws.region", String.class).orElse("NOT SET"));

        System.out.println("quarkus.s3.path-style-access = " +
            config.getOptionalValue("quarkus.s3.path-style-access", String.class).orElse("NOT SET"));

        System.out.println("=== All S3 Properties ===");
        config.getPropertyNames().forEach(name -> {
            if (name.contains("s3")) {
                String value = config.getOptionalValue(name, String.class).orElse("EMPTY/NULL");
                System.out.println(name + " = [" + value + "]");
            }
        });
    }
}
