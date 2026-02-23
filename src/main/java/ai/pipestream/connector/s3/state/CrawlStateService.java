package ai.pipestream.connector.s3.state;

import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * CRUD service for crawl state rows.
 */
@ApplicationScoped
public class CrawlStateService {

    public CrawlStateService() {
    }

    @WithTransaction
    public Uni<CrawlStateEntity> createState(CrawlStateEntity state) {
        if (state == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("state is required"));
        }
        return state.persist().replaceWith(state);
    }

    @WithTransaction
    public Uni<CrawlStateEntity> getState(Long id) {
        if (id == null || id <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("id must be a positive number"));
        }
        return CrawlStateEntity.<CrawlStateEntity>findById(id)
            .onItem().ifNull().failWith(() -> new IllegalStateException("No crawl state found for id=" + id));
    }

    @WithTransaction
    public Uni<java.util.List<CrawlStateEntity>> listStatesForDatasource(String datasourceId) {
        if (datasourceId == null || datasourceId.isBlank()) {
            return Uni.createFrom().failure(new IllegalArgumentException("datasourceId is required"));
        }
        return CrawlStateEntity.find("datasourceId", datasourceId).list();
    }

    @WithTransaction
    public Uni<Void> deleteState(Long id) {
        if (id == null || id <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("id must be a positive number"));
        }
        return CrawlStateEntity.deleteById(id)
            .flatMap(deleted -> deleted
                ? Uni.createFrom().voidItem()
                : Uni.createFrom().failure(new IllegalStateException("No crawl state found for id=" + id)))
            .replaceWithVoid();
    }

    @WithTransaction
    public Uni<CrawlStateEntity> updateState(Long id, java.util.function.Consumer<CrawlStateEntity> updater) {
        if (id == null || id <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("id must be a positive number"));
        }
        if (updater == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("updater is required"));
        }

        return getState(id).flatMap(state -> {
            updater.accept(state);
            state.touchUpdatedAt();
            return Uni.createFrom().item(state);
        });
    }
}

