package ai.pipestream.connector.s3.rest;

import ai.pipestream.connector.s3.entity.DatasourceConfigEntity;
import ai.pipestream.connector.s3.service.DatasourceConfigService;
import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;

/**
 * REST resource for listing and updating S3 datasource registrations.
 * This is an operator-only resource as it includes secrets like API keys.
 */
@Path("/api/datasources")
@Produces(MediaType.APPLICATION_JSON)
public class DatasourceResource {

    /**
     * Default constructor for DatasourceResource.
     */
    public DatasourceResource() {
    }

    @Inject
    DatasourceConfigService datasourceConfigService;

    @Inject
    ObjectMapper objectMapper;

    /**
     * Lists all registered datasources.
     *
     * @return list of datasource configurations
     */
    @GET
    public Uni<List<DatasourceConfigDto>> list() {
        return DatasourceConfigEntity.<DatasourceConfigEntity>listAll()
            .map(list -> list.stream().map(this::toDto).toList());
    }

    /**
     * Gets a specific datasource configuration by ID.
     *
     * @param id datasource identifier
     * @return the datasource configuration
     */
    @GET
    @Path("/{id}")
    public Uni<DatasourceConfigDto> get(@PathParam("id") String id) {
        return datasourceConfigService.getDatasourceConfig(id)
            .map(dc -> new DatasourceConfigDto(
                dc.datasourceId(),
                dc.apiKey(),
                toJsonNode(dc.s3Config())
            ))
            .onFailure(IllegalStateException.class)
            .transform(e -> new NotFoundException("Datasource not found: " + id));
    }

    /**
     * Registers or updates a datasource configuration.
     *
     * @param id            datasource identifier
     * @param headerApiKey  API key from headers
     * @param body          request body with connection details
     * @return the updated datasource configuration
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<DatasourceConfigDto> put(
        @PathParam("id") String id,
        @HeaderParam("x-api-key") String headerApiKey,
        PutDatasourceRequest body
    ) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        if (headerApiKey == null || headerApiKey.isBlank()) {
            throw new IllegalArgumentException("x-api-key header is required");
        }
        final S3ConnectionConfig connectionConfig;
        try {
            connectionConfig = S3ProtoJson.parseConnectionConfig(objectMapper, body.connectionConfig);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid connection_config: " + e.getMessage(), e);
        }
        return datasourceConfigService.registerDatasourceConfig(id, headerApiKey, connectionConfig)
            .flatMap(v -> datasourceConfigService.getDatasourceConfig(id))
            .map(dc -> new DatasourceConfigDto(
                dc.datasourceId(),
                dc.apiKey(),
                toJsonNode(dc.s3Config())
            ));
    }

    private DatasourceConfigDto toDto(DatasourceConfigEntity e) {
        try {
            return new DatasourceConfigDto(e.datasourceId, e.apiKey, objectMapper.readTree(e.s3ConfigJson));
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to read s3 config for " + e.datasourceId, ex);
        }
    }

    private JsonNode toJsonNode(ai.pipestream.connector.s3.v1.S3ConnectionConfig cfg) {
        try {
            return objectMapper.readTree(
                com.google.protobuf.util.JsonFormat.printer().print(cfg)
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize S3 config", e);
        }
    }

    /**
     * Request DTO for updating a datasource.
     */
    public static class PutDatasourceRequest {
        /**
         * S3 connection configuration in JSON format.
         */
        public JsonNode connectionConfig;

        /**
         * Default constructor for PutDatasourceRequest.
         */
        public PutDatasourceRequest() {
        }
    }

    /**
     * DTO representing a datasource configuration.
     *
     * @param datasourceId datasource identifier
     * @param apiKey       API key for authentication
     * @param s3Config     S3 connection configuration
     */
    public record DatasourceConfigDto(String datasourceId, String apiKey, JsonNode s3Config) {
    }
}

