package ai.pipestream.connector.s3.client;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import ai.pipestream.server.util.ChunkSizeCalculator;
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

@ApplicationScoped
public class ConnectorIntakeClient {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeClient.class);

    private final HttpClient httpClient;

    @Inject
    S3ConnectorConfig config;

    @Inject
    ChunkSizeCalculator chunkSizeCalculator;

    public ConnectorIntakeClient() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

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
        int chunkSize = chunkSizeCalculator.calculateChunkSize(sizeBytes);
        LOG.infof("Starting upload for %s (size: %d bytes, chunk size: %d bytes)", sourceUrl, sizeBytes, chunkSize);
        
        HttpRequest.BodyPublisher bodyPublisher = new InputStreamBodyPublisher(bodyInputStream, sizeBytes, chunkSize);

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri)
            .timeout(config.intake().requestTimeout())
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

    private record InputStreamBodyPublisher(InputStream inputStream, long contentLength, int bufferSize,
                                            Executor executor) implements HttpRequest.BodyPublisher {
            InputStreamBodyPublisher(InputStream inputStream, long contentLength, int bufferSize) {
                this(inputStream, contentLength, bufferSize, Infrastructure.getDefaultExecutor());
            }

            @Override
            public long contentLength() {
                return contentLength;
            }

            @Override
            public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                Objects.requireNonNull(subscriber, "subscriber");
                InputStreamSubscription subscription = new InputStreamSubscription(subscriber, inputStream, bufferSize, executor);
                subscriber.onSubscribe(subscription);
            }

            private static final class InputStreamSubscription implements Flow.Subscription {
                private final Flow.Subscriber<? super ByteBuffer> subscriber;
                private final InputStream inputStream;
                private final int bufferSize;
                private final Executor executor;
                private final AtomicLong demand = new AtomicLong(0);
                private final AtomicBoolean started = new AtomicBoolean(false);
                private final Object lock = new Object();
                private volatile boolean cancelled = false;

                InputStreamSubscription(Flow.Subscriber<? super ByteBuffer> subscriber,
                                        InputStream inputStream,
                                        int bufferSize,
                                        Executor executor) {
                    this.subscriber = subscriber;
                    this.inputStream = inputStream;
                    this.bufferSize = bufferSize;
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
                    byte[] buffer = new byte[bufferSize];
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
                    }
                }
            }
        }

    public record IntakeUploadResponse(int statusCode, String contentType, String body) {}
}