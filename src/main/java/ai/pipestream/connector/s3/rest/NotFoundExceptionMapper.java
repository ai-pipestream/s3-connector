package ai.pipestream.connector.s3.rest;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;

/**
 * Maps {@link NotFoundException} to a JAX-RS {@link Response} with status 404 (Not Found).
 */
@Provider
public class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {

    /**
     * Default constructor for NotFoundExceptionMapper.
     */
    public NotFoundExceptionMapper() {
    }

    @Override
    public Response toResponse(NotFoundException exception) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", exception.getMessage()))
            .build();
    }
}
