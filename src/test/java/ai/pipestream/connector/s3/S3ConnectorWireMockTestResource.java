package ai.pipestream.connector.s3;

import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

/**
 * Local wrapper around ConnectorIntakeWireMockTestResource that guarantees
 * the REST client URL and /uploads/raw stub are present for s3-connector tests.
 */
public class S3ConnectorWireMockTestResource extends ConnectorIntakeWireMockTestResource {

    @Override
    public Map<String, String> start() {
        Map<String, String> config = new HashMap<>(super.start());
        String host = config.get("wiremock.host");
        String port = config.get("wiremock.port");
        config.put("quarkus.rest-client.connector-intake.url", "http://" + host + ":" + port);
        registerUploadRawStub(host, port);
        return config;
    }

    private void registerUploadRawStub(String host, String port) {
        String stub = """
                {
                  "request": { "method": "POST", "url": "/uploads/raw" },
                  "response": {
                    "status": 200,
                    "body": "{\\"status\\":\\"accepted\\"}",
                    "headers": { "Content-Type": "application/json" }
                  }
                }
                """;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + host + ":" + port + "/__admin/mappings"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(stub))
                .build();
        try {
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new IllegalStateException("Failed to register /uploads/raw stub. HTTP " + response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while registering /uploads/raw stub", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to register /uploads/raw stub", e);
        }
    }
}
