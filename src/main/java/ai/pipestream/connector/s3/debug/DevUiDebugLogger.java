package ai.pipestream.connector.s3.debug;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.runtime.LaunchMode;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Debug-mode instrumentation: records Dev UI / Compose Dev Services context at startup (NDJSON).
 */
@ApplicationScoped
public class DevUiDebugLogger {

    private static final Path DEBUG_LOG =
            Path.of("/work/core-services/repository-service/.cursor/debug-bcfa42.log");

    @Inject
    LaunchMode launchMode;

    @Inject
    ObjectMapper objectMapper;

    // #region agent log
    void onStart(@Observes StartupEvent ev) {
        try {
            var cfg = ConfigProvider.getConfig();
            String profile =
                    cfg.getOptionalValue("quarkus.profile", String.class).orElse("default");
            boolean composeEnabled =
                    cfg.getOptionalValue("quarkus.compose.devservices.enabled", Boolean.class)
                            .orElse(true);
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            boolean composeYml = cl.getResource("compose-devservices.yml") != null;
            boolean composeYaml = cl.getResource("compose-devservices.yaml") != null;
            boolean dockerHost = Optional.ofNullable(System.getenv("DOCKER_HOST")).isPresent();

            ObjectNode data = objectMapper.createObjectNode();
            data.put("launchMode", launchMode.name());
            data.put("quarkusProfile", profile);
            data.put("composeDevservicesEnabled", composeEnabled);
            data.put("composeDevservicesYmlClasspath", composeYml);
            data.put("composeDevservicesYamlClasspath", composeYaml);
            data.put("dockerHostEnvSet", dockerHost);
            // Same strings Quarkus uses for "Compose Dev Services" footer (DevUIProcessor.processFooterLogs):
            // RPC registers (name with spaces removed) + "Log"; metadata incorrectly uses raw name + "Log".
            final String composeStyleName = "Compose Dev Services";
            data.put("quarkusFooterRpcSuffixRegistered", composeStyleName.replaceAll(" ", "") + "Log");
            data.put("quarkusFooterMetadataSuffixSentToBrowser", composeStyleName + "Log");

            ObjectNode root = objectMapper.createObjectNode();
            root.put("sessionId", "bcfa42");
            root.put("timestamp", System.currentTimeMillis());
            root.put("runId", "startup");
            root.put("hypothesisId", "H1-H4");
            root.put("location", "DevUiDebugLogger.java:onStart");
            root.put("message", "dev-ui compose/footer context at startup");
            root.set("data", data);

            Files.createDirectories(DEBUG_LOG.getParent());
            Files.writeString(
                    DEBUG_LOG,
                    objectMapper.writeValueAsString(root) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.err.println("[DevUiDebugLogger] failed to write debug log: " + e);
        }
    }
    // #endregion
}
