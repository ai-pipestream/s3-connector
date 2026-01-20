package ai.pipestream.connector.s3.grpc;

import io.grpc.Context;

/**
 * Container for gRPC request-scoped metadata and context keys.
 * <p>
 * This utility class provides {@link Context.Key} instances for storing
 * and retrieving request-scoped metadata that is extracted from gRPC
 * request headers. The context keys enable passing authentication and
 * identification information through the gRPC interceptor chain.
 * </p>
 *
 * <h2>Context Keys</h2>
 * <ul>
 *   <li>{@link #API_KEY} - API key for request authentication</li>
 *   <li>{@link #DATASOURCE_ID} - Datasource identifier for request scoping</li>
 * </ul>
 *
 * @since 1.0.0
 */
public final class GrpcRequestContext {

    /**
     * Context key for storing the API key extracted from gRPC request headers.
     * <p>
     * Used by interceptors to pass authentication information through the
     * request processing chain. The API key is typically extracted from
     * the "x-api-key" header.
     * </p>
     */
    public static final Context.Key<String> API_KEY = Context.key("s3.connector.api-key");

    /**
     * Context key for storing the datasource ID extracted from gRPC request headers.
     * <p>
     * Used by interceptors to pass datasource identification information through
     * the request processing chain. The datasource ID is typically extracted from
     * the "x-datasource-id" header and used for request scoping and authorization.
     * </p>
     */
    public static final Context.Key<String> DATASOURCE_ID = Context.key("s3.connector.datasource-id");

    /**
     * Private constructor to prevent instantiation.
     * <p>
     * This class contains only static members and should not be instantiated.
     * </p>
     */
    private GrpcRequestContext() {
    }
}
