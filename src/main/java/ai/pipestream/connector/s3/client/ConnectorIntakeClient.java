package ai.pipestream.connector.s3.client;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.util.UUID;

/**
 * HTTP client for uploading S3 objects to the connector-intake-service.
 * Delegates to a Quarkus declarative REST client for TLS and service discovery support.
 */
@ApplicationScoped
public class ConnectorIntakeClient {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeClient.class);

    @Inject
    @RestClient
    ConnectorIntakeRestClient restClient;

    /**
     * Uploads an S3 object to the connector-intake-service as a raw binary stream.
     *
     * @param datasourceId    datasource identifier for the upload
     * @param apiKey          API key for authentication
     * @param sourceUrl       source URL of the S3 object
     * @param bucket          S3 bucket name
     * @param key             S3 object key
     * @param contentType     MIME content type of the object
     * @param sizeBytes       size of the object in bytes
     * @param bodyInputStream input stream of the object body
     * @return the intake service response
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

        LOG.infof("Starting upload for %s (size: %d bytes)", sourceUrl, sizeBytes);

        String requestId = UUID.randomUUID().toString();

        return restClient.uploadRaw(
                bodyInputStream,
                contentType,
                sizeBytes,
                datasourceId,
                apiKey,
                sourceUrl,
                key,
                key,
                requestId
            )
            .map(response -> {
                String respContentType = response.getHeaderString("content-type");
                if (respContentType == null || respContentType.isBlank()) {
                    respContentType = MediaType.APPLICATION_JSON;
                }
                String respBody = response.readEntity(String.class);
                return new IntakeUploadResponse(response.getStatus(), respContentType, respBody);
            })
            .onFailure().invoke(error -> {
                LOG.errorf(error, "Failed to upload to connector-intake-service: datasourceId=%s, sourceUrl=%s",
                    datasourceId, sourceUrl);
            });
    }

    /**
     * Response from the connector-intake-service upload endpoint.
     *
     * @param statusCode  HTTP status code
     * @param contentType response content type
     * @param body        response body
     */
    public record IntakeUploadResponse(int statusCode, String contentType, String body) {}
}
