package ai.pipestream.connector.s3;

import ai.pipestream.connector.s3.target.CrawlTargetEntity;
import ai.pipestream.connector.s3.target.CrawlTargetMode;
import ai.pipestream.connector.s3.target.CrawlTargetService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.vertx.RunOnVertxContext;
import io.quarkus.test.vertx.UniAsserter;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CRUD coverage for crawl target configuration using reactive Hibernate.
 */
@QuarkusTest
class S3CrawlTargetCrudTest {

    @Inject
    CrawlTargetService crawlTargetService;

    @Test
    @RunOnVertxContext
    void testCrawlTargetCreateReadUpdateDelete(UniAsserter asserter) {
        final String suffix = String.valueOf(System.currentTimeMillis());
        final String datasourceId = "datasource-target-crud-" + suffix;
        final Long[] targetId = new Long[1];
        final Long[] siblingTargetId = new Long[1];

        CrawlTargetService.CrawlTargetSpec createSpec = new CrawlTargetService.CrawlTargetSpec(
            datasourceId,
            "primary-target",
            "bucket-primary",
            "incoming/",
            CrawlTargetMode.INITIAL,
            1,
            1000
        );

        asserter.execute(() -> crawlTargetService.createTarget(createSpec)
            .invoke(created -> targetId[0] = created.id));

        asserter.assertThat(
            () -> crawlTargetService.getTarget(targetId[0]),
            target -> {
                assertThat(target).isNotNull();
                assertThat(target.id).isNotNull();
                assertThat(target.datasourceId).isEqualTo(datasourceId);
                assertThat(target.targetName).isEqualTo("primary-target");
                assertThat(target.bucket).isEqualTo("bucket-primary");
                assertThat(target.objectPrefix).isEqualTo("incoming/");
                assertThat(target.crawlMode).isEqualTo(CrawlTargetMode.INITIAL);
                assertThat(target.failureAllowance).isEqualTo(1);
            }
        );

        CrawlTargetService.CrawlTargetSpec updateSpec = new CrawlTargetService.CrawlTargetSpec(
            datasourceId,
            "primary-target-updated",
            "bucket-primary",
            "archive/",
            CrawlTargetMode.INCREMENTAL,
            2,
            500
        );

        asserter.execute(() -> crawlTargetService.updateTarget(targetId[0], updateSpec));

        asserter.assertThat(
            () -> crawlTargetService.getTarget(targetId[0]),
            target -> {
                assertThat(target.targetName).isEqualTo("primary-target-updated");
                assertThat(target.objectPrefix).isEqualTo("archive/");
                assertThat(target.crawlMode).isEqualTo(CrawlTargetMode.INCREMENTAL);
                assertThat(target.failureAllowance).isEqualTo(2);
            }
        );

        CrawlTargetService.CrawlTargetSpec siblingSpec = new CrawlTargetService.CrawlTargetSpec(
            datasourceId,
            "secondary-target",
            "bucket-secondary",
            null,
            CrawlTargetMode.BOTH,
            1,
            250
        );

        asserter.execute(() -> crawlTargetService.createTarget(siblingSpec)
            .invoke(created -> siblingTargetId[0] = created.id));

        asserter.assertThat(
            () -> crawlTargetService.listTargetsForDatasource(datasourceId),
            targets -> {
                assertThat(targets).hasSize(2);
                assertThat(targets).extracting(target -> target.targetName).containsExactlyInAnyOrder(
                    "primary-target-updated",
                    "secondary-target"
                );
            }
        );

        asserter.execute(() -> crawlTargetService.deleteTarget(siblingTargetId[0]));

        asserter.assertThat(
            () -> crawlTargetService.listTargetsForDatasource(datasourceId),
            targets -> assertThat(targets).hasSize(1)
        );

        asserter.execute(() -> crawlTargetService.deleteTarget(targetId[0]));

        asserter.assertFailedWith(
            () -> crawlTargetService.getTarget(targetId[0]),
            throwable -> assertThat(throwable).isInstanceOf(IllegalStateException.class)
        );
    }

    @Test
    @RunOnVertxContext
    void testCrawlTargetValidation(UniAsserter asserter) {
        assertThatThrownBy(() -> new CrawlTargetService.CrawlTargetSpec(
            "",
            "name",
            "bucket",
            null,
            CrawlTargetMode.INITIAL,
            1,
            1000
        )).isInstanceOf(IllegalArgumentException.class);

        asserter.assertFailedWith(
            () -> crawlTargetService.getTarget(0L),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );

        asserter.assertFailedWith(
            () -> crawlTargetService.deleteTarget(0L),
            throwable -> assertThat(throwable).isInstanceOf(IllegalArgumentException.class)
        );
    }
}

