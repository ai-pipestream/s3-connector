package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.state.CrawlSource;
import ai.pipestream.connector.s3.state.CrawlStateEntity;
import ai.pipestream.connector.s3.state.CrawlStateStatus;
import ai.pipestream.connector.s3.state.CrawlStateService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CRUD coverage for crawl state tracking with reactive Panache.
 */
@QuarkusTest
class S3CrawlStateCrudTest {

    @Inject
    CrawlStateService crawlStateService;

    @Test
    @RunOnVertxContext
    void testCrawlStateLifecycle(UniAsserter asserter) {
        final String suffix = String.valueOf(System.currentTimeMillis());
        final Long[] stateId = new Long[1];

        asserter.execute(() -> {
            CrawlStateEntity entity = new CrawlStateEntity(
                "datasource-state-" + suffix,
                "bucket-state-" + suffix,
                "docs/file.txt",
                "v1",
                "etag-state",
                1234L,
                OffsetDateTime.now(),
                "fingerprint-state"
            );
            entity.crawlSource = CrawlSource.LIVE;
            return crawlStateService.createState(entity)
                .invoke(() -> stateId[0] = entity.id);
        });

        asserter.assertThat(
            () -> crawlStateService.getState(stateId[0]),
            state -> {
                assertThat(state).isNotNull();
                assertThat(state.datasourceId).isEqualTo("datasource-state-" + suffix);
                assertThat(state.bucket).isEqualTo("bucket-state-" + suffix);
                assertThat(state.objectKey).isEqualTo("docs/file.txt");
                assertThat(state.status).isEqualTo(CrawlStateStatus.PENDING);
                assertThat(state.failureAllowance).isEqualTo(1);
                assertThat(state.crawlSource).isEqualTo(CrawlSource.LIVE);
            }
        );

        asserter.execute(() -> crawlStateService.updateState(stateId[0], state -> {
            state.markAttemptStarted();
            state.attemptCount = 2;
            state.markFailed("temporary", OffsetDateTime.now().plusMinutes(5));
        }));

        asserter.assertThat(
            () -> crawlStateService.getState(stateId[0]),
            state -> assertThat(state).isNotNull().satisfies(s -> {
                assertThat(s.status).isEqualTo(CrawlStateStatus.EXHAUSTED);
                assertThat(s.isRetryExhausted()).isTrue();
                assertThat(s.lastError).contains("temporary");
            })
        );

        asserter.execute(() -> crawlStateService.deleteState(stateId[0]));

        asserter.assertThat(
            () -> crawlStateService.listStatesForDatasource("datasource-state-" + suffix),
            rows -> assertThat(rows).isEmpty()
        );
    }
}

