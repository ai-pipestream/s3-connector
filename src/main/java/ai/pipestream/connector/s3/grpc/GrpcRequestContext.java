package ai.pipestream.connector.s3.grpc;

import io.grpc.Context;

/**
 * Holds gRPC request-scoped metadata extracted from headers.
 */
public final class GrpcRequestContext {

    public static final Context.Key<String> API_KEY = Context.key("s3.connector.api-key");
    public static final Context.Key<String> DATASOURCE_ID = Context.key("s3.connector.datasource-id");

    private GrpcRequestContext() {
    }
}
