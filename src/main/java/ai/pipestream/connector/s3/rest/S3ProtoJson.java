package ai.pipestream.connector.s3.rest;

import ai.pipestream.connector.s3.v1.S3ConnectionConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * Parses {@link S3ConnectionConfig} from JSON using the same protobuf JSON mapping as gRPC / persistence.
 */
public final class S3ProtoJson {

    private S3ProtoJson() {
    }

    public static S3ConnectionConfig parseConnectionConfig(ObjectMapper objectMapper, JsonNode node)
        throws InvalidProtocolBufferException {
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException("connection_config is required");
        }
        try {
            S3ConnectionConfig.Builder builder = S3ConnectionConfig.newBuilder();
            JsonFormat.parser().merge(objectMapper.writeValueAsString(node), builder);
            return builder.build();
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid connection_config JSON: " + e.getMessage(), e);
        }
    }

    public static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
