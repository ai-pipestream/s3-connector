package ai.pipestream.connector.s3.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * gRPC server interceptor that extracts authentication and identification metadata from request headers.
 * <p>
 * This interceptor processes incoming gRPC requests and extracts the API key and datasource ID
 * from standard headers, storing them in the gRPC context for use by downstream handlers.
 * The extracted values are made available through {@link GrpcRequestContext} keys.
 * </p>
 *
 * <h2>Extracted Headers</h2>
 * <ul>
 *   <li>{@code x-api-key} - API key for request authentication</li>
 *   <li>{@code x-datasource-id} - Datasource identifier for request scoping</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <p>
 * As a {@link GlobalInterceptor}, this interceptor is automatically applied to all
 * gRPC service methods in the application. The extracted context values can be
 * accessed using {@link Context#current()} and the appropriate context keys.
 * </p>
 *
 * @since 1.0.0
 */
@GlobalInterceptor
@ApplicationScoped
public class S3ApiKeyInterceptor implements ServerInterceptor {

    /**
     * Default constructor for CDI injection.
     */
    public S3ApiKeyInterceptor() {
    }

    /**
     * Metadata key for the "x-api-key" header.
     * <p>
     * Used to extract the API key from incoming gRPC request metadata.
     * The header value is expected to be an ASCII string.
     * </p>
     */
    private static final Metadata.Key<String> API_KEY_HEADER =
        Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Metadata key for the "x-datasource-id" header.
     * <p>
     * Used to extract the datasource identifier from incoming gRPC request metadata.
     * The header value is expected to be an ASCII string.
     * </p>
     */
    private static final Metadata.Key<String> DATASOURCE_ID_HEADER =
        Metadata.Key.of("x-datasource-id", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Intercepts gRPC calls to extract authentication and identification metadata.
     * <p>
     * This method is called for every incoming gRPC request. It extracts the API key
     * and datasource ID from the request headers and stores them in the gRPC context.
     * The context is then propagated to the next handler in the chain.
     * </p>
     *
     * @param <ReqT> the request message type
     * @param <RespT> the response message type
     * @param call the server call being intercepted
     * @param headers the metadata headers from the request
     * @param next the next handler in the interceptor chain
     * @return a server call listener that will handle the request
     * @since 1.0.0
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        String apiKey = headers.get(API_KEY_HEADER);
        String datasourceId = headers.get(DATASOURCE_ID_HEADER);

        Context context = Context.current()
            .withValue(GrpcRequestContext.API_KEY, apiKey)
            .withValue(GrpcRequestContext.DATASOURCE_ID, datasourceId);

        return Contexts.interceptCall(context, call, headers, next);
    }
}
