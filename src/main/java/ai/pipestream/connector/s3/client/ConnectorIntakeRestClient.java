package ai.pipestream.connector.s3.client;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.io.InputStream;

/**
 * Declarative REST client for uploading S3 objects to the connector-intake-service.
 */
@RegisterRestClient(configKey = "connector-intake")
@Path("/uploads/raw")
public interface ConnectorIntakeRestClient {

    /**
     * Uploads an S3 object to the connector-intake-service as a raw binary stream.
     *
     * @param body          input stream of the object body
     * @param contentType   MIME content type of the object
     * @param contentLength size of the object in bytes
     * @param datasourceId  datasource identifier for the upload
     * @param apiKey        API key for authentication
     * @param sourceUri     source URI of the S3 object
     * @param sourcePath    source path of the S3 object
     * @param filename      filename for the object
     * @param requestId     request identifier for the upload
     * @return the intake service response
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    Uni<Response> uploadRaw(
        InputStream body,
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Content-Length") long contentLength,
        @HeaderParam("x-datasource-id") String datasourceId,
        @HeaderParam("x-api-key") String apiKey,
        @HeaderParam("x-source-uri") String sourceUri,
        @HeaderParam("x-source-path") String sourcePath,
        @HeaderParam("x-filename") String filename,
        @HeaderParam("x-request-id") String requestId
    );
}
