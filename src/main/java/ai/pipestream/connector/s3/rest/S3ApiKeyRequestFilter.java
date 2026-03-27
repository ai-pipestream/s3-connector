package ai.pipestream.connector.s3.rest;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;

/**
 * Requires {@code x-api-key} for HTTP {@code /api/*} routes except {@code POST /api/control/test-bucket},
 * matching gRPC: {@code startCrawl} requires a key; {@code testBucketCrawl} does not validate one.
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class S3ApiKeyRequestFilter implements ContainerRequestFilter {

    /**
     * Default constructor for S3ApiKeyRequestFilter.
     */
    public S3ApiKeyRequestFilter() {
    }

    private static final String HDR = "x-api-key";

    @Override
    public void filter(ContainerRequestContext requestContext) {
        String path = requestContext.getUriInfo().getPath(true);
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.startsWith("/api/")) {
            return;
        }
        if ("POST".equals(requestContext.getMethod()) && path.contains("control/test-bucket")) {
            return;
        }
        String apiKey = requestContext.getHeaderString(HDR);
        if (apiKey == null || apiKey.isBlank()) {
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
                .entity(Map.of("error", "x-api-key header is required"))
                .build());
        }
    }
}
