package ai.pipestream.connector.s3.rest;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;

/**
 * Maps {@link IllegalArgumentException} to a JAX-RS {@link Response} with status 400 (Bad Request).
 */
@Provider
public class IllegalArgumentExceptionMapper implements ExceptionMapper<IllegalArgumentException> {

    /**
     * Default constructor for IllegalArgumentExceptionMapper.
     */
    public IllegalArgumentExceptionMapper() {
    }

    @Override
    public Response toResponse(IllegalArgumentException exception) {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(Map.of("error", exception.getMessage()))
            .build();
    }
}
