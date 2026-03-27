package ai.pipestream.connector.s3.rest;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;

/**
 * Maps {@link IllegalStateException} to a JAX-RS {@link Response} with status 404 (Not Found).
 * This is used when an operation is requested on a resource that is in an invalid state,
 * often implying it was not found or is no longer available.
 */
@Provider
public class IllegalStateExceptionMapper implements ExceptionMapper<IllegalStateException> {

    /**
     * Default constructor for IllegalStateExceptionMapper.
     */
    public IllegalStateExceptionMapper() {
    }

    @Override
    public Response toResponse(IllegalStateException exception) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", exception.getMessage()))
            .build();
    }
}
