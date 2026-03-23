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
 * CRUD for {@link CrawlTargetEntity} via {@link CrawlTargetService}.
 */
@Path("/api/crawl-targets")
@Produces(MediaType.APPLICATION_JSON)
public class CrawlTargetResource {

    @Inject
    CrawlTargetService crawlTargetService;

    @GET
    public Uni<List<CrawlTargetDto>> list(@QueryParam("datasourceId") String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId query parameter is required");
        }
        return crawlTargetService.listTargetsForDatasource(datasourceId).map(list -> list.stream().map(CrawlTargetDto::fromEntity).toList());
    }

    @GET
    @Path("/{id}")
    public Uni<CrawlTargetDto> get(@PathParam("id") long id) {
        return crawlTargetService.getTarget(id).map(CrawlTargetDto::fromEntity);
    }

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

    @DELETE
    @Path("/{id}")
    public Uni<Response> delete(@PathParam("id") long id) {
        return crawlTargetService.deleteTarget(id).replaceWith(Response.noContent().build());
    }

    public static class CrawlTargetRequest {
        public String datasourceId;
        public String targetName;
        public String bucket;
        public String objectPrefix;
        public String mode = "INITIAL";
        public int failureAllowance = 1;
        public int maxKeysPerRequest = 1000;
    }

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
