package ai.pipestream.connector.s3.client;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP client for uploading S3 objects to the connector-intake-service.
 * <p>
 * This client handles the HTTP communication with the connector-intake-service
 * for uploading crawled S3 objects. It performs raw HTTP POST requests to the
 * {@code /uploads/raw} endpoint with appropriate headers for object metadata.
 * </p>
 *
 * <h2>Features</h2>
 * <ul>
 *   <li>Streaming upload of S3 objects with known content length</li>
 *   <li>Reactive API using Mutiny Uni</li>
 *   <li>Automatic header injection for datasource identification</li>
 *   <li>Configurable timeouts and endpoints</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * This class is thread-safe and can be used concurrently from multiple threads.
 * The underlying {@link HttpClient} instance is shared and immutable.
 * </p>
 *
 * @since 1.0.0
 */
@ApplicationScoped
public class ConnectorIntakeClient {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeClient.class);
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private final HttpClient httpClient;

    @Inject
    S3ConnectorConfig config;

    /**
     * Creates a new ConnectorIntakeClient with default HTTP client configuration.
     * <p>
     * Initializes an HTTP client with HTTP/1.1 version for compatibility
     * with the connector-intake-service.
     * </p>
     */
    public ConnectorIntakeClient() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    /**
     * Uploads an S3 object to the connector-intake-service via HTTP raw upload.
     * <p>
     * This method streams the S3 object content as a raw HTTP POST request body
     * to the configured intake service endpoint. All necessary headers are automatically
     * injected for proper object identification and processing.
     * </p>
     *
     * <h4>Headers Injected</h4>
     * <ul>
     *   <li>{@code x-datasource-id} - Datasource identifier</li>
     *   <li>{@code x-api-key} - API key for authentication</li>
     *   <li>{@code x-source-uri} - S3 source URL</li>
     *   <li>{@code x-source-path} - S3 object key</li>
     *   <li>{@code x-filename} - S3 object key (for filename)</li>
     *   <li>{@code x-request-id} - Unique request identifier</li>
     *   <li>{@code Content-Type} - Object content type (if provided)</li>
     *   <li>{@code Content-Length} - Object size in bytes</li>
     * </ul>
     *
     * @param datasourceId the datasource identifier for tracking and authorization
     * @param apiKey the API key for authenticating with the intake service
     * @param sourceUrl the S3 source URL in format "s3://bucket/key"
     * @param bucket the S3 bucket name (for reference)
     * @param key the S3 object key (used for headers)
     * @param contentType the MIME content type of the object, may be null
     * @param sizeBytes the size of the object in bytes
     * @param bodyInputStream the input stream containing the object content
     * @return a {@link Uni} that completes with the upload response containing
     *         HTTP status code, content type, and response body
     * @throws IllegalArgumentException if any required parameter is null or invalid
     */
    public Uni<IntakeUploadResponse> uploadRaw(
        String datasourceId,
        String apiKey,
        String sourceUrl,
        String bucket,
        String key,
        String contentType,
        long sizeBytes,
        InputStream bodyInputStream) {

        URI uri = buildUploadUri();
        HttpRequest.BodyPublisher bodyPublisher = new InputStreamBodyPublisher(bodyInputStream, sizeBytes);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri)
            .timeout(config.intake().requestTimeout())
            .header("Content-Length", String.valueOf(sizeBytes))
            .header("x-datasource-id", datasourceId)
            .header("x-api-key", apiKey)
            .header("x-source-uri", sourceUrl)
            .header("x-source-path", key)
            .header("x-filename", key)
            .header("x-request-id", UUID.randomUUID().toString());

        if (contentType != null && !contentType.isBlank()) {
            requestBuilder.header("Content-Type", contentType);
        }

        HttpRequest request = requestBuilder.POST(bodyPublisher).build();

        LOG.debugf("Uploading to connector-intake-service: datasourceId=%s, sourceUrl=%s", datasourceId, sourceUrl);

        return Uni.createFrom().completionStage(
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        ).map(response -> {
            String responseContentType = response.headers()
                .firstValue("content-type")
                .orElse(MediaType.APPLICATION_JSON);
            return new IntakeUploadResponse(response.statusCode(), responseContentType, response.body());
        }).onFailure().invoke(error -> {
            LOG.errorf(error, "Failed to upload to connector-intake-service: datasourceId=%s, sourceUrl=%s", 
                datasourceId, sourceUrl);
        });
    }

    private URI buildUploadUri() {
        String baseUrl = config.intake().baseUrl();
        String rawPath = config.intake().rawPath();
        if (baseUrl.endsWith("/") && rawPath.startsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return URI.create(baseUrl + rawPath);
    }

    /**
     * {@link HttpRequest.BodyPublisher} implementation that streams an {@link InputStream} with a known content length.
     * <p>
     * This publisher enables efficient streaming of S3 object content to HTTP requests
     * without loading the entire object into memory. It implements the reactive streams
     * {@link Flow.Publisher} interface for integration with the HTTP client's asynchronous API.
     * </p>
     *
     * <h3>Thread Safety</h3>
     * <p>
     * Instances of this class are immutable and thread-safe. The underlying
     * {@link InputStream} is accessed sequentially during the streaming process.
     * </p>
     *
     * @param inputStream the input stream containing the data to publish
     * @param contentLength the total content length in bytes
     * @param executor the executor for asynchronous processing
     * @since 1.0.0
     */
    private record InputStreamBodyPublisher(InputStream inputStream, long contentLength,
                                            Executor executor) implements HttpRequest.BodyPublisher {
            /**
             * Creates a new InputStreamBodyPublisher with the default executor.
             *
             * @param inputStream the input stream containing the data to publish
             * @param contentLength the total content length in bytes
             */
            InputStreamBodyPublisher(InputStream inputStream, long contentLength) {
                this(inputStream, contentLength, Infrastructure.getDefaultExecutor());
            }

            /**
             * Creates a new InputStreamBodyPublisher with a custom executor.
             *
             * @param inputStream the input stream containing the data to publish
             * @param contentLength the total content length in bytes
             * @param executor the executor for asynchronous processing
             */
            private InputStreamBodyPublisher(InputStream inputStream, long contentLength, Executor executor) {
                this.inputStream = Objects.requireNonNull(inputStream, "inputStream");
                this.contentLength = contentLength;
                this.executor = Objects.requireNonNull(executor, "executor");
            }

            /**
             * Subscribes a subscriber to this publisher.
             * <p>
             * Creates a new subscription and begins the streaming process.
             * The subscriber will receive {@link ByteBuffer} chunks of data
             * until the stream is exhausted or an error occurs.
             * </p>
             *
             * @param subscriber the subscriber to receive the data chunks
             * @throws NullPointerException if subscriber is null
             */
            @Override
            public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                Objects.requireNonNull(subscriber, "subscriber");
                InputStreamSubscription subscription = new InputStreamSubscription(subscriber, inputStream, executor);
                subscriber.onSubscribe(subscription);
            }

            /**
             * Flow.Subscription implementation for streaming InputStream data.
             * <p>
             * Manages the backpressure-aware streaming of data from an InputStream
             * to a Flow.Subscriber. Implements demand-driven flow control and
             * proper resource cleanup.
             * </p>
             */
            private static final class InputStreamSubscription implements Flow.Subscription {
                private final Flow.Subscriber<? super ByteBuffer> subscriber;
                private final InputStream inputStream;
                private final Executor executor;
                /** Demand counter for backpressure control. */
                private final AtomicLong demand = new AtomicLong(0);
                /** Flag indicating if streaming has started. */
                private final AtomicBoolean started = new AtomicBoolean(false);
                /** Lock for coordinating demand and cancellation. */
                private final Object lock = new Object();
                /** Flag indicating if the subscription has been cancelled. */
                private volatile boolean cancelled = false;

                /**
                 * Creates a new InputStreamSubscription.
                 *
                 * @param subscriber the subscriber to receive data
                 * @param inputStream the input stream to read from
                 * @param executor the executor for asynchronous processing
                 */
                InputStreamSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
                                        InputStream inputStream,
                                        Executor executor) {
                    this.subscriber = subscriber;
                    this.inputStream = inputStream;
                    this.executor = executor;
                }

                /**
                 * Requests additional data from the publisher.
                 * <p>
                 * Implements backpressure by allowing the subscriber to request
                 * a specific number of data chunks. If this is the first request,
                 * streaming begins asynchronously.
                 * </p>
                 *
                 * @param n the number of data chunks requested (must be > 0)
                 * @throws IllegalArgumentException if n <= 0
                 */
                @Override
                public void request(long n) {
                    if (n <= 0) {
                        subscriber.onError(new IllegalArgumentException("Request must be > 0"));
                        cancel();
                        return;
                    }
                    if (cancelled) {
                        return;
                    }
                    demand.getAndAccumulate(n, InputStreamSubscription::addWithCap);
                    if (started.compareAndSet(false, true)) {
                        executor.execute(this::pump);
                    } else {
                        synchronized (lock) {
                            lock.notifyAll();
                        }
                    }
                }

                /**
                 * Cancels the subscription and releases resources.
                 * <p>
                 * Signals that the subscriber no longer wants to receive data.
                 * Closes the input stream and notifies any waiting threads.
                 * </p>
                 */
                @Override
                public void cancel() {
                    cancelled = true;
                    closeQuietly();
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }

                /**
                 * Pumps data from the InputStream to the subscriber.
                 * <p>
                 * Reads data in chunks from the input stream and publishes them
                 * to the subscriber, respecting backpressure demands. Continues
                 * until the stream is exhausted, cancelled, or an error occurs.
                 * </p>
                 */
                private void pump() {
                    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                    try {
                        while (!cancelled) {
                            if (demand.get() <= 0) {
                                synchronized (lock) {
                                    while (!cancelled && demand.get() <= 0) {
                                        lock.wait();
                                    }
                                }
                            }
                            if (cancelled) {
                                return;
                            }
                            int read = inputStream.read(buffer);
                            if (read < 0) {
                                subscriber.onComplete();
                                return;
                            }
                            ByteBuffer payload = ByteBuffer.wrap(copyOf(buffer, read));
                            demand.decrementAndGet();
                            subscriber.onNext(payload);
                        }
                    } catch (IOException e) {
                        subscriber.onError(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        subscriber.onError(e);
                    } finally {
                        closeQuietly();
                    }
                }

                /**
                 * Adds two long values with overflow protection.
                 * <p>
                 * Adds the given values and caps the result at Long.MAX_VALUE
                 * to prevent overflow issues in demand calculations.
                 * </p>
                 *
                 * @param current the current demand value
                 * @param n the amount to add
                 * @return the sum, capped at Long.MAX_VALUE
                 */
                private static long addWithCap(long current, long n) {
                    long sum = current + n;
                    return sum < 0 ? Long.MAX_VALUE : sum;
                }

                /**
                 * Creates a copy of a byte array with exact length.
                 * <p>
                 * Used to create properly sized byte arrays from the read buffer
                 * when the actual read length is less than the buffer size.
                 * </p>
                 *
                 * @param buffer the source buffer
                 * @param length the number of bytes to copy
                 * @return a new byte array containing the copied data
                 */
                private static byte[] copyOf(byte[] buffer, int length) {
                    byte[] copy = new byte[length];
                    System.arraycopy(buffer, 0, copy, 0, length);
                    return copy;
                }

                /**
                 * Closes the input stream quietly.
                 * <p>
                 * Attempts to close the input stream, ignoring any IOException
                 * that may occur during the close operation.
                 * </p>
                 */
                private void closeQuietly() {
                    try {
                        inputStream.close();
                    } catch (IOException ignored) {
                        // best-effort close
                    }
                }
            }
        }

    /**
     * Response from an intake service upload operation.
     * <p>
     * Contains the HTTP response details from uploading an object to the
     * connector-intake-service, including status code and response content.
     *
     * @param statusCode the HTTP status code of the response
     * @param contentType the content type of the response body
     * @param body the response body content as a string
     * @since 1.0.0
     */
    public record IntakeUploadResponse(int statusCode, String contentType, String body) {}
}
