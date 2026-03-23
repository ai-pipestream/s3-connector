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
 * Read/create/delete for {@link CrawlStateEntity} via {@link CrawlStateService}.
 */
@Path("/api/crawl-states")
@Produces(MediaType.APPLICATION_JSON)
public class CrawlStateResource {

    @Inject
    CrawlStateService crawlStateService;

    @GET
    public Uni<List<CrawlStateDto>> list(@QueryParam("datasourceId") String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId query parameter is required");
        }
        return crawlStateService.listStatesForDatasource(datasourceId)
            .map(list -> list.stream().map(CrawlStateDto::fromEntity).toList());
    }

    @GET
    @Path("/{id}")
    public Uni<CrawlStateDto> get(@PathParam("id") long id) {
        return crawlStateService.getState(id).map(CrawlStateDto::fromEntity);
    }

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

    @DELETE
    @Path("/{id}")
    public Uni<Response> delete(@PathParam("id") long id) {
        return crawlStateService.deleteState(id).replaceWith(Response.noContent().build());
    }

    public static class CrawlStateCreateRequest {
        public String datasourceId;
        public String bucket;
        public String objectKey;
        public String objectVersionId;
        public String objectEtag;
        public long sizeBytes;
        public OffsetDateTime lastModified;
        public String fingerprint;
        public String status;
        public int failureAllowance;
        public String crawlSource;
    }

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
