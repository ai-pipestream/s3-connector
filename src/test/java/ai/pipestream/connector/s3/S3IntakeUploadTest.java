package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.events.S3CrawlEventPublisher;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import ai.pipestream.test.support.S3TestResource;
import ai.pipestream.test.support.S3WithSampleDataTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the complete S3 crawl event processing pipeline.
 * <p>
 * This test validates the full flow from Kafka event consumption through
 * S3 object downloading to intake service upload using WireMock.
 * </p>
 *
 * <h2>Test Flow</h2>
 * <pre>
 * 1. Load saved S3CrawlEvent protobufs from src/test/resources/sample-crawl-events/
 * 2. Publish events to Kafka (s3-crawl-events-intake-upload-test topic - unique to this test)
 * 3. S3CrawlEventConsumer processes events:
 *    - Downloads objects from S3 (SeaweedFS)
 *    - Uploads to WireMock intake service
 * 4. Verify processing completes without errors
 * </pre>
 *
 * <h2>Test Data</h2>
 * <ul>
 *   <li>Uses saved protobuf events from S3CrawlEventCaptureTest</li>
 *   <li>S3 (SeaweedFS) contains sample-documents uploaded by S3WithSampleDataTestResource</li>
 *   <li>WireMock mocks the connector-intake-service</li>
 * </ul>
 *
 * <h2>Kafka Isolation</h2>
 * <p>
 * This test uses a unique Kafka topic (s3-crawl-events-intake-upload-test) to avoid
 * cross-contamination with other tests. Each test class should use its own topic.
 * </p>
 *
 * @since 1.0.0
 */
@QuarkusTest
@TestProfile(S3IntakeUploadTest.IntakeUploadTestProfile.class)
@QuarkusTestResource(S3WithSampleDataTestResource.class)
@QuarkusTestResource(S3ConnectorWireMockTestResource.class)
class S3IntakeUploadTest {

    private static final Logger LOG = Logger.getLogger(S3IntakeUploadTest.class);

    /**
     * Test profile that configures a unique Kafka topic for this test.
     * Uses a random suffix to ensure the topic is unique per test RUN (not just per test class).
     */
    public static class IntakeUploadTestProfile implements QuarkusTestProfile {
        private static final String UNIQUE_TOPIC = "s3-crawl-events-intake-upload-test-" +
            java.util.UUID.randomUUID().toString().substring(0, 8);

        @Override
        public java.util.Map<String, String> getConfigOverrides() {
            return java.util.Map.of(
                "mp.messaging.incoming.s3-crawl-events-in.enabled", "true",
                "mp.messaging.outgoing.s3-crawl-events-out.topic", UNIQUE_TOPIC,
                "mp.messaging.incoming.s3-crawl-events-in.topic", UNIQUE_TOPIC,
                "mp.messaging.incoming.s3-crawl-events-in.failure-strategy", "fail"
            );
        }
    }

    @Inject
    S3CrawlEventPublisher eventPublisher;

    @Inject
    DatasourceConfigService datasourceConfigService;

    @ConfigProperty(name = "wiremock.host")
    String wiremockHost;

    @ConfigProperty(name = "wiremock.port")
    String wiremockPort;

    private static final String DATASOURCE_ID = "test-intake-datasource";
    private static final String API_KEY = "test-intake-api-key";
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @BeforeEach
    void waitForS3Data() {
        List<String> keys = List.of(
                "sample_audio/sample.mp3",
                "sample_text/sample.txt",
                "sample_image/sample.png");

        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3TestResource.ACCESS_KEY, S3TestResource.SECRET_KEY)))
                .region(Region.of("us-east-1"))
                .endpointOverride(URI.create(S3TestResource.getSharedEndpoint()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {

            await()
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> keys.stream().allMatch(key -> {
                        try {
                            s3.headObject(r -> r.bucket(S3TestResource.BUCKET).key(key));
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }));
        }
    }

    @BeforeEach
    void stubUploadEndpoint() throws Exception {
        String stub = """
                {
                  "request": { "method": "POST", "url": "/uploads/raw" },
                  "response": { "status": 200, "body": "{\\"status\\":\\"accepted\\"}", "headers": { "Content-Type": "application/json" } }
                }
                """;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + wiremockHost + ":" + wiremockPort + "/__admin/mappings"))
                .POST(HttpRequest.BodyPublishers.ofString(stub))
                .header("Content-Type", "application/json")
                .build();
        HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Tests the full pipeline from loading saved events to intake upload.
     * <p>
     * This test:
     * 1. Registers datasource configuration
     * 2. Loads a subset of saved S3CrawlEvent protobufs
     * 3. Publishes them to Kafka
     * 4. Waits for consumer processing
     * 5. Verifies processing completes (consumer logs show uploads)
     * </p>
     */
    @Test
    @RunOnVertxContext
    void testIntakeUploadPipeline(UniAsserter asserter) throws Exception {
        // Register datasource configuration
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId(S3TestResource.ACCESS_KEY)
            .setSecretAccessKey(S3TestResource.SECRET_KEY)
            .setRegion("us-east-1")
            .setEndpointOverride(S3TestResource.getSharedEndpoint())
            .setPathStyleAccess(true)
            .build();

        asserter.execute(() ->
            datasourceConfigService.registerDatasourceConfig(DATASOURCE_ID, API_KEY, s3Config));

        // Create fresh events for files we KNOW exist in S3 (uploaded by S3WithSampleDataTestResource)
        // Instead of loading saved events which may reference different keys/buckets
        List<S3CrawlEvent> testEvents = java.util.Arrays.asList(
            S3CrawlEvent.newBuilder()
                .setEventId("test-event-1")
                .setDatasourceId(DATASOURCE_ID)
                .setBucket(S3TestResource.BUCKET)
                .setKey("sample_audio/sample.mp3")
                .setSourceUrl("s3://" + S3TestResource.BUCKET + "/sample_audio/sample.mp3")
                .setSizeBytes(1000)
                .build(),
            S3CrawlEvent.newBuilder()
                .setEventId("test-event-2")
                .setDatasourceId(DATASOURCE_ID)
                .setBucket(S3TestResource.BUCKET)
                .setKey("sample_text/sample.txt")
                .setSourceUrl("s3://" + S3TestResource.BUCKET + "/sample_text/sample.txt")
                .setSizeBytes(500)
                .build(),
            S3CrawlEvent.newBuilder()
                .setEventId("test-event-3")
                .setDatasourceId(DATASOURCE_ID)
                .setBucket(S3TestResource.BUCKET)
                .setKey("sample_image/sample.png")
                .setSourceUrl("s3://" + S3TestResource.BUCKET + "/sample_image/sample.png")
                .setSizeBytes(2000)
                .build()
        );
        final int expectedEventCount = testEvents.size();

        asserter.execute(() -> {
            LOG.info("\n=== S3 Intake Upload Pipeline Test ===");
            LOG.infof("Testing with %d fresh S3CrawlEvents", expectedEventCount);
        });

        // Publish events to Kafka
        for (S3CrawlEvent event : testEvents) {
            asserter.execute(() -> {
                LOG.infof("Publishing event: %s", event.getSourceUrl());
                return eventPublisher.publish(event);
            });
        }

        // Wait for consumer to process events and upload to WireMock
        asserter.execute(() -> {
            LOG.infof("Waiting for %d events to be processed and uploaded...", expectedEventCount);

            // Use Awaitility for non-blocking wait (it will poll until condition is met or timeout)
            await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    int uploadCount = getWireMockUploadCount();
                    LOG.infof("  Current upload count: %d / %d", uploadCount, expectedEventCount);
                    assertThat(uploadCount)
                        .as("WireMock should receive all uploaded events")
                        .isGreaterThanOrEqualTo(expectedEventCount);
                });

            LOG.info("✓ All events successfully processed and uploaded!");
            LOG.info("=== Test Complete ===");
        });
    }

    /**
     * Tests error handling when S3 objects don't exist.
     * <p>
     * Creates synthetic crawl events for non-existent objects and verifies
     * that the pipeline handles errors gracefully without crashing.
     * </p>
     */
    @Test
    @RunOnVertxContext
    void testErrorHandlingForMissingObjects(UniAsserter asserter) {
        // Register datasource configuration
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId(S3TestResource.ACCESS_KEY)
            .setSecretAccessKey(S3TestResource.SECRET_KEY)
            .setRegion("us-east-1")
            .setEndpointOverride(S3TestResource.getSharedEndpoint())
            .setPathStyleAccess(true)
            .build();

        asserter.execute(() ->
            datasourceConfigService.registerDatasourceConfig(DATASOURCE_ID, API_KEY, s3Config));

        // Create event for non-existent object
        S3CrawlEvent missingEvent = S3CrawlEvent.newBuilder()
            .setEventId("test-missing-object-event")
            .setDatasourceId(DATASOURCE_ID)
            .setBucket(S3TestResource.BUCKET)
            .setKey("non-existent-file.txt")
            .setSourceUrl("s3://" + S3TestResource.BUCKET + "/non-existent-file.txt")
            .setSizeBytes(0)
            .build();

        // Publish the event - should not crash the consumer
        asserter.execute(() -> {
            LOG.info("Publishing event for non-existent object (testing error handling)");
            return eventPublisher.publish(missingEvent);
        });

        // Wait and verify NO upload was made for the missing object
        asserter.execute(() -> {
            LOG.info("Waiting to verify missing object is not uploaded...");

            // Wait a bit to ensure consumer had time to process (and fail)
            await()
                .pollDelay(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> {
                    // We expect 0 uploads for this specific path
                    int uploadCount = getWireMockUploadCountForPath("non-existent-file.txt");
                    assertThat(uploadCount)
                        .as("Missing objects should not be uploaded to intake service")
                        .isEqualTo(0);
                });

            LOG.info("✓ Error handling verified: Consumer handled missing object gracefully");
        });
    }

    /**
     * Loads saved S3CrawlEvent protobuf files from src/test/resources/sample-crawl-events/.
     * <p>
     * These files were created by S3CrawlEventCaptureTest and represent real
     * crawl events from the sample-documents jar.
     * </p>
     *
     * @param limit maximum number of events to load (for test performance)
     * @return list of deserialized S3CrawlEvent objects
     * @throws IOException if files cannot be read
     */
    private List<S3CrawlEvent> loadSavedEvents(int limit) throws IOException {
        List<S3CrawlEvent> events = new ArrayList<>();

        Path resourcesDir = Paths.get(System.getProperty("user.dir"))
            .resolve("src/test/resources/sample-crawl-events");

        if (!Files.exists(resourcesDir)) {
            LOG.warnf("WARNING: Sample events directory not found: %s", resourcesDir);
            LOG.info("Run S3CrawlEventCaptureTest first to generate sample events");
            return events;
        }

        try (Stream<Path> paths = Files.list(resourcesDir)) {
            paths.filter(p -> p.toString().endsWith(".pb"))
                .limit(limit)
                .forEach(eventFile -> {
                    try {
                        byte[] eventBytes = Files.readAllBytes(eventFile);
                        S3CrawlEvent event = S3CrawlEvent.parseFrom(eventBytes);
                        events.add(event);
                        LOG.infof("Loaded event: %s (%d bytes)",
                            event.getSourceUrl(), event.getSizeBytes());
                    } catch (IOException e) {
                        LOG.errorf("Failed to load event file: %s - %s",
                            eventFile.getFileName(), e.getMessage());
                    }
                });
        }

        return events;
    }

    /**
     * Gets the count of requests to WireMock's /uploads/raw endpoint.
     * <p>
     * Queries WireMock's admin API to count how many upload requests have been received.
     * </p>
     *
     * @return the number of upload requests received by WireMock
     */
    private int getWireMockUploadCount() {
        try {
            String adminUrl = String.format("http://%s:%s/__admin/requests", wiremockHost, wiremockPort);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(adminUrl))
                .GET()
                .build();

            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String body = response.body();
                // Simple count of "url" : "/uploads/raw" occurrences
                // This is a simple approach; could use JSON parser for robustness
                int count = 0;
                String searchPattern = "\"/uploads/raw\"";
                int index = 0;
                while ((index = body.indexOf(searchPattern, index)) != -1) {
                    count++;
                    index += searchPattern.length();
                }
                return count;
            }
            return 0;
        } catch (Exception e) {
            LOG.errorf("Failed to query WireMock admin API: %s", e.getMessage());
            return 0;
        }
    }

    /**
     * Gets the count of upload requests for a specific source path.
     *
     * @param sourcePath the source path to filter by (e.g., "non-existent-file.txt")
     * @return the number of upload requests with the specified source path
     */
    private int getWireMockUploadCountForPath(String sourcePath) {
        try {
            String adminUrl = String.format("http://%s:%s/__admin/requests", wiremockHost, wiremockPort);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(adminUrl))
                .GET()
                .build();

            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String body = response.body();
                // Count occurrences of both /uploads/raw and the specific source path
                int count = 0;
                int index = 0;
                while ((index = body.indexOf("\"/uploads/raw\"", index)) != -1) {
                    // Check if this request has our source path nearby
                    int pathIndex = body.indexOf("\"x-source-path\" : \"" + sourcePath + "\"", index);
                    if (pathIndex > index && pathIndex < index + 500) { // Within reasonable distance
                        count++;
                    }
                    index += 10;
                }
                return count;
            }
            return 0;
        } catch (Exception e) {
            LOG.errorf("Failed to query WireMock admin API: %s", e.getMessage());
            return 0;
        }
    }
}
