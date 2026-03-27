package ai.pipestream.connector.s3.rest;

import ai.pipestream.connector.s3.state.CrawlSource;
import ai.pipestream.connector.s3.state.CrawlStateEntity;
import ai.pipestream.connector.s3.state.CrawlStateService;
import ai.pipestream.connector.s3.state.CrawlStateStatus;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * REST resource for managing {@link CrawlStateEntity} via {@link CrawlStateService}.
 * Provides endpoints for listing, getting, creating, and deleting crawl states.
 */
@Path("/api/crawl-states")
@Produces(MediaType.APPLICATION_JSON)
public class CrawlStateResource {

    /**
     * Default constructor for CrawlStateResource.
     */
    public CrawlStateResource() {
    }

    @Inject
    CrawlStateService crawlStateService;

    /**
     * Lists crawl states for a specific datasource.
     *
     * @param datasourceId datasource identifier
     * @return list of crawl states
     */
    @GET
    public Uni<List<CrawlStateDto>> list(@QueryParam("datasourceId") String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId query parameter is required");
        }
        return crawlStateService.listStatesForDatasource(datasourceId)
            .map(list -> list.stream().map(CrawlStateDto::fromEntity).toList());
    }

    /**
     * Gets a specific crawl state by ID.
     *
     * @param id crawl state ID
     * @return the crawl state
     */
    @GET
    @Path("/{id}")
    public Uni<CrawlStateDto> get(@PathParam("id") long id) {
        return crawlStateService.getState(id).map(CrawlStateDto::fromEntity);
    }

    /**
     * Creates a new crawl state.
     *
     * @param body request body with crawl state details
     * @return the created crawl state
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Uni<CrawlStateDto> create(CrawlStateCreateRequest body) {
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }
        String versionId = body.objectVersionId != null ? body.objectVersionId : "";
        CrawlStateEntity state = new CrawlStateEntity(
            body.datasourceId,
            body.bucket,
            body.objectKey,
            versionId,
            body.objectEtag,
            body.sizeBytes,
            body.lastModified,
            body.fingerprint
        );
        if (body.status != null) {
            state.status = CrawlStateStatus.valueOf(body.status);
        }
        if (body.failureAllowance > 0) {
            state.failureAllowance = body.failureAllowance;
        }
        if (body.crawlSource != null) {
            state.crawlSource = CrawlSource.valueOf(body.crawlSource);
        }
        return crawlStateService.createState(state).map(CrawlStateDto::fromEntity);
    }

    /**
     * Deletes a crawl state by ID.
     *
     * @param id crawl state ID
     * @return empty response
     */
    @DELETE
    @Path("/{id}")
    public Uni<Response> delete(@PathParam("id") long id) {
        return crawlStateService.deleteState(id).replaceWith(Response.noContent().build());
    }

    /**
     * Request DTO for creating a crawl state.
     */
    public static class CrawlStateCreateRequest {
        /**
         * Datasource identifier.
         */
        public String datasourceId;
        /**
         * S3 bucket name.
         */
        public String bucket;
        /**
         * S3 object key.
         */
        public String objectKey;
        /**
         * Optional S3 object version ID.
         */
        public String objectVersionId;
        /**
         * S3 object ETag.
         */
        public String objectEtag;
        /**
         * S3 object size in bytes.
         */
        public long sizeBytes;
        /**
         * S3 object last modified timestamp.
         */
        public OffsetDateTime lastModified;
        /**
         * Object fingerprint for change detection.
         */
        public String fingerprint;
        /**
         * Initial status of the crawl state.
         */
        public String status;
        /**
         * Number of failures allowed before giving up.
         */
        public int failureAllowance;
        /**
         * Source of the crawl (e.g., INITIAL, EVENT).
         */
        public String crawlSource;

        /**
         * Default constructor for CrawlStateCreateRequest.
         */
        public CrawlStateCreateRequest() {
        }
    }

    /**
     * DTO representing a crawl state.
     *
     * @param id                database ID
     * @param datasourceId      datasource identifier
     * @param bucket            S3 bucket name
     * @param objectKey         S3 object key
     * @param objectVersionId   S3 object version ID
     * @param objectEtag        S3 object ETag
     * @param sizeBytes         S3 object size in bytes
     * @param lastModified      S3 object last modified timestamp
     * @param status            current status
     * @param attemptCount      number of attempts made
     * @param failureAllowance   max failures allowed
     * @param crawlSource       source of the crawl
     * @param nextRetryAt       scheduled next retry timestamp
     * @param lastAttemptAt     timestamp of last attempt
     * @param completedAt       timestamp of completion
     * @param fingerprint       object fingerprint
     * @param lastError         last error message encountered
     * @param updatedAt         last update timestamp
     * @param createdAt         creation timestamp
     */
    public record CrawlStateDto(
        Long id,
        String datasourceId,
        String bucket,
        String objectKey,
        String objectVersionId,
        String objectEtag,
        long sizeBytes,
        OffsetDateTime lastModified,
        String status,
        int attemptCount,
        int failureAllowance,
        String crawlSource,
        OffsetDateTime nextRetryAt,
        OffsetDateTime lastAttemptAt,
        OffsetDateTime completedAt,
        String fingerprint,
        String lastError,
        OffsetDateTime updatedAt,
        OffsetDateTime createdAt
    ) {
        /**
         * Creates a DTO from a {@link CrawlStateEntity}.
         *
         * @param e the entity
         * @return the DTO
         */
        static CrawlStateDto fromEntity(CrawlStateEntity e) {
            return new CrawlStateDto(
                e.id,
                e.datasourceId,
                e.bucket,
                e.objectKey,
                e.objectVersionId,
                e.objectEtag,
                e.sizeBytes,
                e.lastModified,
                e.status.name(),
                e.attemptCount,
                e.failureAllowance,
                e.crawlSource.name(),
                e.nextRetryAt,
                e.lastAttemptAt,
                e.completedAt,
                e.fingerprint,
                e.lastError,
                e.updatedAt,
                e.createdAt
            );
        }
    }
}

