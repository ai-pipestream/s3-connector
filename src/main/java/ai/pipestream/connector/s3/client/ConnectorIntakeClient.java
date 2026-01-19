package ai.pipestream.connector.s3.client;

import ai.pipestream.connector.s3.config.S3ConnectorConfig;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * HTTP client for uploading objects to connector-intake-service.
 * <p>
 * Performs multipart uploads via HTTP POST to /uploads/raw endpoint.
 */
@ApplicationScoped
public class ConnectorIntakeClient {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeClient.class);

    private final HttpClient httpClient;

    @Inject
    S3ConnectorConfig config;

    public ConnectorIntakeClient() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    /**
     * Upload an S3 object to connector-intake-service via HTTP multipart upload.
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
        String boundary = generateBoundary();
        HttpRequest.BodyPublisher bodyPublisher = createMultipartBody(
            boundary, datasourceId, apiKey, sourceUrl, bucket, key, contentType, sizeBytes, bodyInputStream);

        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(config.intake().requestTimeout())
            .header("Content-Type", "multipart/form-data; boundary=" + boundary)
            .POST(bodyPublisher)
            .build();

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

    private String generateBoundary() {
        return "----s3-connector-boundary-" + System.currentTimeMillis();
    }

    private HttpRequest.BodyPublisher createMultipartBody(
        String boundary,
        String datasourceId,
        String apiKey,
        String sourceUrl,
        String bucket,
        String key,
        String contentType,
        long sizeBytes,
        InputStream bodyInputStream) {

        // TODO: Implement proper multipart/form-data body publisher
        // For now, use a simple streaming body publisher
        // In production, should use proper multipart encoding with boundaries
        
        // This is a simplified version - full implementation would need:
        // 1. Proper multipart boundary encoding
        // 2. Streaming of file content
        // 3. Proper header encoding
        
        // For MVP, we'll use InputStreamBodyPublisher similar to connector-intake-service
        return HttpRequest.BodyPublishers.ofInputStream(() -> bodyInputStream);
    }

    public record IntakeUploadResponse(int statusCode, String contentType, String body) {}
}
