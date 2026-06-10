package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.state.CrawlStatusRegistry;
import ai.pipestream.connector.s3.v1.MutinyS3ConnectorControlServiceGrpc;
import ai.pipestream.connector.s3.v1.S3CrawlPhase;
import ai.pipestream.connector.s3.v1.StreamCrawlStatusRequest;
import ai.pipestream.connector.s3.v1.StreamCrawlStatusResponse;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * StreamCrawlStatus semantics: state-snapshot first (no subscribe race),
 * updates on movement, completion at terminal phase, NOT_FOUND for
 * unknown crawls. Drives the registry directly — no S3 involved.
 */
@QuarkusTest
class CrawlStatusStreamTest {

    @Inject
    CrawlStatusRegistry registry;

    private MutinyS3ConnectorControlServiceGrpc.MutinyS3ConnectorControlServiceStub controlService;
    private io.grpc.ManagedChannel channel;

    @org.junit.jupiter.api.BeforeEach
    void setupGrpcClient() {
        org.eclipse.microprofile.config.Config config =
                org.eclipse.microprofile.config.ConfigProvider.getConfig();
        int port = config.getOptionalValue("quarkus.grpc.server.test-port", Integer.class)
                .or(() -> config.getOptionalValue("quarkus.grpc.server.port", Integer.class))
                .orElseThrow();
        channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext().build();
        controlService = MutinyS3ConnectorControlServiceGrpc.newMutinyStub(channel);
    }

    @org.junit.jupiter.api.AfterEach
    void teardownChannel() {
        if (channel != null) channel.shutdownNow();
    }

    @Test
    void unknownCrawlFailsNotFound() {
        AssertSubscriber<StreamCrawlStatusResponse> sub = controlService
                .streamCrawlStatus(StreamCrawlStatusRequest.newBuilder()
                        .setRequestId("never-started").build())
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        sub.awaitFailure(Duration.ofSeconds(5));
        assertThat(sub.getFailure())
                .as("unknown request_id must fail loud, not hang or emit empties")
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("NOT_FOUND");
    }

    @Test
    void lateSubscriberGetsTerminalSnapshotAndCompletes() {
        String id = "crawl-" + UUID.randomUUID();
        registry.register(id);
        registry.markDispatched(id);
        registry.markDispatched(id);
        registry.complete(id);

        AssertSubscriber<StreamCrawlStatusResponse> sub = controlService
                .streamCrawlStatus(StreamCrawlStatusRequest.newBuilder().setRequestId(id).build())
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        sub.awaitCompletion(Duration.ofSeconds(5));
        List<StreamCrawlStatusResponse> items = sub.getItems();
        assertThat(items).as("a subscriber arriving AFTER completion still learns the outcome").hasSize(1);
        StreamCrawlStatusResponse snapshot = items.get(0);
        assertThat(snapshot.getPhase()).isEqualTo(S3CrawlPhase.S3_CRAWL_PHASE_COMPLETED);
        assertThat(snapshot.getDispatchedCount()).isEqualTo(2);
        assertThat(snapshot.getTotalCount()).isEqualTo(2);
    }

    @Test
    void liveSubscriberSeesProgressThenCompletion() {
        String id = "crawl-" + UUID.randomUUID();
        registry.register(id);

        AssertSubscriber<StreamCrawlStatusResponse> sub = controlService
                .streamCrawlStatus(StreamCrawlStatusRequest.newBuilder().setRequestId(id).build())
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        // First snapshot: LISTING with zero dispatched.
        sub.awaitItems(1, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(0).getPhase()).isEqualTo(S3CrawlPhase.S3_CRAWL_PHASE_LISTING);
        assertThat(sub.getItems().get(0).getDispatchedCount()).isZero();

        registry.markDispatched(id);
        registry.markDispatched(id);
        registry.markDispatched(id);
        sub.awaitItems(2, Duration.ofSeconds(5));

        registry.complete(id);
        sub.awaitCompletion(Duration.ofSeconds(5));

        StreamCrawlStatusResponse last = sub.getItems().get(sub.getItems().size() - 1);
        assertThat(last.getPhase()).isEqualTo(S3CrawlPhase.S3_CRAWL_PHASE_COMPLETED);
        assertThat(last.getDispatchedCount()).isEqualTo(3);
        assertThat(last.getTotalCount()).isEqualTo(3);
    }

    @Test
    void failureCarriesErrorAndPartialCount() {
        String id = "crawl-" + UUID.randomUUID();
        registry.register(id);
        registry.markDispatched(id);
        registry.fail(id, "S3Exception: bucket vanished");

        AssertSubscriber<StreamCrawlStatusResponse> sub = controlService
                .streamCrawlStatus(StreamCrawlStatusRequest.newBuilder().setRequestId(id).build())
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        sub.awaitCompletion(Duration.ofSeconds(5));
        StreamCrawlStatusResponse snapshot = sub.getItems().get(0);
        assertThat(snapshot.getPhase()).isEqualTo(S3CrawlPhase.S3_CRAWL_PHASE_FAILED);
        assertThat(snapshot.getError()).contains("bucket vanished");
        assertThat(snapshot.getDispatchedCount())
                .as("failure reports how far the crawl got")
                .isEqualTo(1);
        assertThat(snapshot.getTotalCount())
                .as("total is never set for a failed crawl (proto3 default 0)")
                .isZero();
    }
}
