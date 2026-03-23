package ai.pipestream.connector.s3.rest;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.util.Map;
import java.util.concurrent.CompletionException;

@Provider
public class CompletionExceptionMapper implements ExceptionMapper<CompletionException> {
    @Override
    public Response toResponse(CompletionException exception) {
        Throwable c = exception.getCause() != null ? exception.getCause() : exception;
        if (c instanceof IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST).entity(Map.of("error", iae.getMessage())).build();
        }
        if (c instanceof IllegalStateException ise) {
            return Response.status(Response.Status.NOT_FOUND).entity(Map.of("error", ise.getMessage())).build();
        }
        if (c instanceof NotFoundException nfe) {
            return Response.status(Response.Status.NOT_FOUND).entity(Map.of("error", nfe.getMessage())).build();
        }
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(Map.of("error", c.getMessage() != null ? c.getMessage() : "Internal error"))
            .build();
    }
}
