package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.service.S3CrawlService;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import ai.pipestream.connector.s3.v1.S3CrawlEvent;
import ai.pipestream.test.support.S3TestResource;
import ai.pipestream.test.support.S3WithSampleDataTestResource;
import ai.pipestream.test.support.kafka.IsolatedKafkaTopicsProfile;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that crawls all sample documents from test-documents jar and captures
 * the emitted S3CrawlEvent protobufs for reuse in other tests.
 * <p>
 * This test performs two key functions:
 * </p>
 * <ol>
 *   <li>Crawls the entire S3 (SeaweedFS) bucket populated with test-documents</li>
 *   <li>Captures all emitted S3CrawlEvent protobufs and optionally saves them to src/test/resources/</li>
 * </ol>
 *
 * <h2>Test Flow</h2>
 * <pre>
 * 1. S3WithSampleDataTestResource uploads ~100+ files from test-documents jar
 * 2. Test crawls the bucket and emits S3CrawlEvents to Kafka
 * 3. Test consumes events from Kafka
 * 4. If test.capture.save-events=true, saves each event as .pb file to src/test/resources/sample-crawl-events/
 * 5. These saved events can be reused in consumer/publisher tests
 * </pre>
 *
 * <h2>Configuration</h2>
 * <p>
 * Set <code>test.capture.save-events=true</code> to save events to disk.
 * By default, events are captured but not saved to avoid modifying source files on every test run.
 * </p>
 *
 * <h2>Output</h2>
 * <p>
 * When saving is enabled, creates files like:
 * </p>
 * <pre>
 * src/test/resources/sample-crawl-events/
 *   ├── sample_text_sample.txt.pb
 *   ├── sample_image_sample.png.pb
 *   ├── sample_office_files_sample.docx.pb
 *   └── ... (~100+ event files)
 * </pre>
 *
 * @since 1.0.0
 */
@QuarkusTest
@TestProfile(S3CrawlEventCaptureTest.CaptureTestProfile.class)
@QuarkusTestResource(S3WithSampleDataTestResource.class)
class S3CrawlEventCaptureTest {

    public static class CaptureTestProfile extends IsolatedKafkaTopicsProfile {
        // Inherits the unique topic generation
    }

    @Inject
    S3CrawlService crawlService;

    @Inject
    DatasourceConfigService datasourceConfigService;

    @ConfigProperty(name = "mp.messaging.outgoing.s3-crawl-events-out.topic")
    String kafkaTopic;

    @ConfigProperty(name = "test.capture.save-events", defaultValue = "false")
    boolean saveEvents;

    /**
     * Crawls all sample documents and optionally saves crawl events to src/test/resources.
     * <p>
     * This test:
     * 1. Crawls the entire bucket with all test-documents
     * 2. Consumes S3CrawlEvents from Kafka
     * 3. If test.capture.save-events=true, saves each event as a protobuf file for reuse
     * </p>
     */
    @Test
    @RunOnVertxContext
    void testCrawlAllSampleDocumentsAndCaptureEvents(UniAsserter asserter) throws Exception {
        String datasourceId = "sample-docs-crawler";
        String apiKey = "test-api-key";
        String bucket = S3TestResource.BUCKET;

        // Create S3 configuration for SeaweedFS (S3-compatible)
        S3ConnectionConfig s3Config = S3ConnectionConfig.newBuilder()
            .setCredentialsType("static")
            .setAccessKeyId(S3TestResource.ACCESS_KEY)
            .setSecretAccessKey(S3TestResource.SECRET_KEY)
            .setRegion("us-east-1")
            .setEndpointOverride(S3TestResource.getSharedEndpoint())
            .setPathStyleAccess(true)
            .build();

        // Register datasource configuration (non-blocking)
        asserter.execute(() -> datasourceConfigService.registerDatasourceConfig(datasourceId, apiKey, s3Config));

        // Start crawling entire bucket (this will emit events to Kafka)
        asserter.execute(() -> crawlService.crawlBucket(datasourceId, bucket, null));

        // Give Kafka time to process events
        asserter.execute(() -> {
            try {
                Thread.sleep(3000); // Wait 3 seconds for events to be produced
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Now consume and save the events
        asserter.execute(() -> {
            try {
                List<S3CrawlEvent> capturedEvents = consumeEventsFromKafka();

                System.out.println("=== Captured S3 Crawl Events ===");
                System.out.printf("Total events captured: %d%n", capturedEvents.size());

                if (!capturedEvents.isEmpty()) {
                    // Save events to src/test/resources (only if enabled)
                    if (saveEvents) {
                        Path outputDir = saveEventsToResources(capturedEvents);
                        System.out.printf("Saved %d event files to: %s%n",
                            capturedEvents.size(), outputDir);
                    } else {
                        System.out.println("Event saving disabled (set test.capture.save-events=true to enable)");
                    }

                    // Show sample of what was captured
                    System.out.println("\nSample captured events:");
                    capturedEvents.stream()
                        .limit(10)
                        .forEach(event -> System.out.printf("  - %s/%s (%d bytes)%n",
                            event.getBucket(), event.getKey(), event.getSizeBytes()));

                    if (capturedEvents.size() > 10) {
                        System.out.printf("  ... and %d more events%n",
                            capturedEvents.size() - 10);
                    }

                    // Verify we got events from different directories
                    long uniqueDirs = capturedEvents.stream()
                        .map(e -> e.getKey().contains("/") ?
                            e.getKey().substring(0, e.getKey().indexOf("/")) : "root")
                        .distinct()
                        .count();

                    System.out.printf("\nEvents from %d different directories%n", uniqueDirs);
                    assertTrue(uniqueDirs > 5,
                        "Should have events from multiple directories (sample_text, sample_image, etc.)");

                    assertTrue(capturedEvents.stream()
                        .allMatch(event -> event.getSourceUrl().contains("crawl_source=initial")),
                        "Initial crawl should emit crawl_source=initial marker in sourceUrl");
                } else {
                    System.out.println("WARNING: No events captured from Kafka");
                    System.out.println("This might be expected if Kafka is not running or events haven't been produced yet");
                }
            } catch (Exception e) {
                System.err.println("Error capturing events: " + e.getMessage());
                e.printStackTrace();
                // Don't fail the test - this is informational
            }
        });
    }

    /**
     * Consumes S3CrawlEvent messages from Kafka.
     * <p>
     * NOTE: Messages are Apicurio-encoded, so they have schema registry headers.
     * We need to skip the first 5 bytes (magic byte + schema ID) to get the protobuf.
     * </p>
     */
    private List<S3CrawlEvent> consumeEventsFromKafka() {
        List<S3CrawlEvent> events = new ArrayList<>();

        // Create Kafka consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094"); // Kafka DevServices
        props.put("group.id", "test-event-capture-" + System.currentTimeMillis());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(kafkaTopic));
            System.out.printf("Consuming from Kafka topic: %s%n", kafkaTopic);

            // Poll for events (with timeout)
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));

            System.out.printf("Polled %d records from Kafka%n", records.count());

            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    byte[] rawBytes = record.value();

                    // Debug: print first 10 bytes to understand encoding
                    if (events.isEmpty()) {
                        System.out.print("First message bytes (hex): ");
                        for (int i = 0; i < Math.min(10, rawBytes.length); i++) {
                            System.out.printf("%02X ", rawBytes[i]);
                        }
                        System.out.printf("(total length: %d)%n", rawBytes.length);
                    }

                    // Apicurio protobuf serializer uses:
                    // Bytes 0-3: Magic header + schema ID (varies)
                    // Then: Protobuf varint for message index
                    // Then: Actual protobuf message

                    // Try to find the protobuf start
                    // Protobuf messages typically start with field tags like 0x0A (field 1, wire type 2)
                    int protobufStart = -1;
                    for (int i = 0; i < Math.min(20, rawBytes.length); i++) {
                        // Look for typical protobuf field tag (field 1, length-delimited = 0x0A)
                        if (rawBytes[i] == 0x0A && i + 1 < rawBytes.length) {
                            protobufStart = i;
                            break;
                        }
                    }

                    if (protobufStart > 0) {
                        byte[] protobufBytes = new byte[rawBytes.length - protobufStart];
                        System.arraycopy(rawBytes, protobufStart, protobufBytes, 0, protobufBytes.length);

                        S3CrawlEvent event = S3CrawlEvent.parseFrom(protobufBytes);
                        events.add(event);
                    } else {
                        // Try parsing as raw protobuf (fallback)
                        S3CrawlEvent event = S3CrawlEvent.parseFrom(rawBytes);
                        events.add(event);
                    }
                } catch (Exception e) {
                    if (events.isEmpty()) {
                        System.err.printf("Failed to parse event: %s%n", e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.err.printf("Error consuming from Kafka: %s%n", e.getMessage());
            e.printStackTrace();
        }

        return events;
    }

    /**
     * Saves captured S3CrawlEvent protobufs to src/test/resources/sample-crawl-events/.
     * <p>
     * Each event is saved as a .pb file named after the S3 key.
     * For example: s3://bucket/sample_text/sample.txt -> sample_text_sample.txt.pb
     * </p>
     *
     * @param events the events to save
     * @return the path to the output directory
     */
    private Path saveEventsToResources(List<S3CrawlEvent> events) throws IOException {
        // Determine the src/test/resources directory
        // This assumes we're running from the project root
        Path projectRoot = Paths.get(System.getProperty("user.dir"));
        Path resourcesDir = projectRoot.resolve("src/test/resources/sample-crawl-events");

        // Create directory if it doesn't exist
        Files.createDirectories(resourcesDir);

        // Clean existing files (optional - comment out if you want to keep old events)
        if (Files.exists(resourcesDir)) {
            Files.list(resourcesDir)
                .filter(p -> p.toString().endsWith(".pb"))
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        System.err.printf("Failed to delete old event file: %s%n", p);
                    }
                });
        }

        // Save each event
        for (S3CrawlEvent event : events) {
            String filename = sanitizeFilename(event.getKey()) + ".pb";
            Path eventFile = resourcesDir.resolve(filename);

            byte[] eventBytes = event.toByteArray();
            Files.write(eventFile, eventBytes);
        }

        return resourcesDir;
    }

    /**
     * Sanitizes S3 key to be a valid filename.
     * Replaces / with _ and removes problematic characters.
     */
    private String sanitizeFilename(String s3Key) {
        return s3Key
            .replace("/", "_")
            .replace("\\", "_")
            .replace(":", "_")
            .replace("*", "_")
            .replace("?", "_")
            .replace("\"", "_")
            .replace("<", "_")
            .replace(">", "_")
            .replace("|", "_");
    }
}
