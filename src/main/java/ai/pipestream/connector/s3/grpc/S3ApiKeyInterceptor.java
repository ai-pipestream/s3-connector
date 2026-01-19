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
 * Extracts API key and datasource ID from gRPC headers.
 */
@GlobalInterceptor
@ApplicationScoped
public class S3ApiKeyInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> API_KEY_HEADER =
        Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATASOURCE_ID_HEADER =
        Metadata.Key.of("x-datasource-id", Metadata.ASCII_STRING_MARSHALLER);

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
