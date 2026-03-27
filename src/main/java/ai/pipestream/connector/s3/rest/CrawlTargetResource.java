package ai.pipestream.connector.s3.rest;

import ai.pipestream.connector.s3.target.CrawlTargetEntity;
import ai.pipestream.connector.s3.target.CrawlTargetMode;
import ai.pipestream.connector.s3.target.CrawlTargetService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * REST resource for managing {@link CrawlTargetEntity} via {@link CrawlTargetService}.
 * Provides CRUD operations for crawl targets.
 */
@Path("/api/crawl-targets")
@Produces(MediaType.APPLICATION_JSON)
public class CrawlTargetResource {

    /**
     * Default constructor for CrawlTargetResource.
     */
    public CrawlTargetResource() {
    }

    @Inject
    CrawlTargetService crawlTargetService;

    /**
     * Lists crawl targets for a specific datasource.
     *
     * @param datasourceId datasource identifier
     * @return list of crawl targets
     */
    @GET
    public Uni<List<CrawlTargetDto>> list(@QueryParam("datasourceId") String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId query parameter is required");
        }
        return crawlTargetService.listTargetsForDatasource(datasourceId).map(list -> list.stream().map(CrawlTargetDto::fromEntity).toList());
    }

    /**
     * Gets a specific crawl target by ID.
     *
     * @param id crawl target ID
     * @return the crawl target
     */
    @GET
    @Path("/{id}")
    public Uni<CrawlTargetDto> get(@PathParam("id") long id) {
        return crawlTargetService.getTarget(id).map(CrawlTargetDto::fromEntity);
    }

    /**
     * Creates a new crawl target.
     *
     * @param body request body with crawl target details
     * @return the created crawl target
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<CrawlTargetDto> create(CrawlTargetRequest body) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        var spec = new CrawlTargetService.CrawlTargetSpec(
            body.datasourceId,
            body.targetName,
            body.bucket,
            body.objectPrefix,
            CrawlTargetMode.valueOf(body.mode),
            body.failureAllowance,
            body.maxKeysPerRequest
        );
        return crawlTargetService.createTarget(spec).map(CrawlTargetDto::fromEntity);
    }

    /**
     * Updates an existing crawl target.
     *
     * @param id   crawl target ID to update
     * @param body request body with updated crawl target details
     * @return the updated crawl target
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<CrawlTargetDto> update(@PathParam("id") long id, CrawlTargetRequest body) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        var spec = new CrawlTargetService.CrawlTargetSpec(
            body.datasourceId,
            body.targetName,
            body.bucket,
            body.objectPrefix,
            CrawlTargetMode.valueOf(body.mode),
            body.failureAllowance,
            body.maxKeysPerRequest
        );
        return crawlTargetService.updateTarget(id, spec).map(CrawlTargetDto::fromEntity);
    }

    /**
     * Deletes a crawl target by ID.
     *
     * @param id crawl target ID to delete
     * @return empty response
     */
    @DELETE
    @Path("/{id}")
    public Uni<Response> delete(@PathParam("id") long id) {
        return crawlTargetService.deleteTarget(id).replaceWith(Response.noContent().build());
    }

    /**
     * Request DTO for creating or updating a crawl target.
     */
    public static class CrawlTargetRequest {
        /**
         * Datasource identifier.
         */
        public String datasourceId;
        /**
         * Descriptive name for the target.
         */
        public String targetName;
        /**
         * S3 bucket name.
         */
        public String bucket;
        /**
         * Optional S3 object prefix filter.
         */
        public String objectPrefix;
        /**
         * Crawl mode (e.g., INITIAL, PERIODIC).
         */
        public String mode = "INITIAL";
        /**
         * Number of failures allowed per object.
         */
        public int failureAllowance = 1;
        /**
         * Maximum number of keys to request from S3 per batch.
         */
        public int maxKeysPerRequest = 1000;

        /**
         * Default constructor for CrawlTargetRequest.
         */
        public CrawlTargetRequest() {
        }
    }

    /**
     * DTO representing a crawl target.
     *
     * @param id                database ID
     * @param datasourceId      datasource identifier
     * @param targetName        target name
     * @param bucket            S3 bucket name
     * @param objectPrefix      S3 object prefix
     * @param crawlMode         crawl mode
     * @param failureAllowance   max failures allowed
     * @param maxKeysPerRequest keys per S3 request
     * @param enabled           whether target is enabled
     * @param lastCrawlAt       timestamp of last crawl
     * @param createdAt         creation timestamp
     * @param updatedAt         last update timestamp
     */
    public record CrawlTargetDto(
        Long id,
        String datasourceId,
        String targetName,
        String bucket,
        String objectPrefix,
        String crawlMode,
        int failureAllowance,
        int maxKeysPerRequest,
        boolean enabled,
        OffsetDateTime lastCrawlAt,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
    ) {
        /**
         * Creates a DTO from a {@link CrawlTargetEntity}.
         *
         * @param e the entity
         * @return the DTO
         */
        static CrawlTargetDto fromEntity(CrawlTargetEntity e) {
            return new CrawlTargetDto(
                e.id,
                e.datasourceId,
                e.targetName,
                e.bucket,
                e.objectPrefix,
                e.crawlMode.name(),
                e.failureAllowance,
                e.maxKeysPerRequest,
                e.enabled,
                e.lastCrawlAt,
                e.createdAt,
                e.updatedAt
            );
        }
    }
}

