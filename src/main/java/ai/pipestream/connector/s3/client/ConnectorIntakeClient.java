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
 * HTTP client for uploading objects to connector-intake-service.
 * <p>
 * Performs raw uploads via HTTP POST to /uploads/raw endpoint with headers.
 */
@ApplicationScoped
public class ConnectorIntakeClient {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeClient.class);
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private final HttpClient httpClient;

    @Inject
    S3ConnectorConfig config;

    public ConnectorIntakeClient() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    /**
     * Upload an S3 object to connector-intake-service via HTTP raw upload.
     * <p>
     * Sends the object content as raw body with required headers.
     *
     * @param datasourceId datasource identifier
     * @param apiKey API key for authentication
     * @param sourceUrl S3 source URL (e.g., "s3://bucket/key")
     * @param bucket S3 bucket name
     * @param key S3 object key
     * @param contentType content type (may be null)
     * @param sizeBytes object size in bytes
     * @param bodyInputStream input stream for object content
     * @return response with status code and body
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
         * HttpClient BodyPublisher that streams an InputStream with a known length.
         */
        private record InputStreamBodyPublisher(InputStream inputStream, long contentLength,
                                                Executor executor) implements HttpRequest.BodyPublisher {
            InputStreamBodyPublisher(InputStream inputStream, long contentLength) {
                this(inputStream, contentLength, Infrastructure.getDefaultExecutor());
            }

            private InputStreamBodyPublisher(InputStream inputStream, long contentLength, Executor executor) {
                this.inputStream = Objects.requireNonNull(inputStream, "inputStream");
                this.contentLength = contentLength;
                this.executor = Objects.requireNonNull(executor, "executor");
            }

            @Override
            public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                Objects.requireNonNull(subscriber, "subscriber");
                InputStreamSubscription subscription = new InputStreamSubscription(subscriber, inputStream, executor);
                subscriber.onSubscribe(subscription);
            }

            private static final class InputStreamSubscription implements Flow.Subscription {
                private final Flow.Subscriber<? super ByteBuffer> subscriber;
                private final InputStream inputStream;
                private final Executor executor;
                private final AtomicLong demand = new AtomicLong(0);
                private final AtomicBoolean started = new AtomicBoolean(false);
                private final Object lock = new Object();
                private volatile boolean cancelled = false;

                InputStreamSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
                                        InputStream inputStream,
                                        Executor executor) {
                    this.subscriber = subscriber;
                    this.inputStream = inputStream;
                    this.executor = executor;
                }

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

                @Override
                public void cancel() {
                    cancelled = true;
                    closeQuietly();
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }

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

                private static long addWithCap(long current, long n) {
                    long sum = current + n;
                    return sum < 0 ? Long.MAX_VALUE : sum;
                }

                private static byte[] copyOf(byte[] buffer, int length) {
                    byte[] copy = new byte[length];
                    System.arraycopy(buffer, 0, copy, 0, length);
                    return copy;
                }

                private void closeQuietly() {
                    try {
                        inputStream.close();
                    } catch (IOException ignored) {
                        // best-effort close
                    }
                }
            }
        }

    public record IntakeUploadResponse(int statusCode, String contentType, String body) {}
}
