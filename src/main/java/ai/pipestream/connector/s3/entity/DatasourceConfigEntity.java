package ai.pipestream.connector.s3.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

/**
 * Database entity for storing S3 datasource configurations.
 * <p>
 * Persists datasource-specific configuration including API keys and S3 connection
 * parameters. This enables configuration persistence across service restarts and
 * sharing configurations between multiple connector instances.
 * </p>
 *
 * <h2>Database Table</h2>
 * <p>Table: {@code s3_datasource_configs}</p>
 *
 * @since 1.0.0
 */
@Entity
@Table(name = "s3_datasource_configs", indexes = {
    @Index(name = "idx_s3_datasource_configs_datasource_id", columnList = "datasource_id"),
    @Index(name = "idx_s3_datasource_configs_created_at", columnList = "created_at")
})
public class DatasourceConfigEntity extends PanacheEntityBase {

    /**
     * Unique identifier for the datasource configuration (primary key).
     */
    @Id
    @Column(name = "datasource_id", unique = true, nullable = false)
    public String datasourceId;

    /**
     * API key for accessing the connector-intake-service.
     * <p>
     * This key is used for authentication when uploading documents
     * to the intake service.
     * </p>
     */
    @Column(name = "api_key", nullable = false)
    public String apiKey;

    /**
     * S3 connection configuration as JSON.
     * <p>
     * Stores the complete S3ConnectionConfig protobuf message as JSON.
     * Includes credentials type, endpoints, regions, and other S3 settings.
     * </p>
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "s3_config", columnDefinition = "jsonb", nullable = false)
    public String s3ConfigJson;

    /**
     * Timestamp when this configuration was first created.
     */
    @Column(name = "created_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime createdAt;

    /**
     * Timestamp when this configuration was last updated.
     */
    @Column(name = "updated_at", columnDefinition = "TIMESTAMP WITH TIME ZONE", nullable = false)
    public OffsetDateTime updatedAt;

    /**
     * Version number for optimistic locking.
     */
    @Version
    @Column(name = "version", nullable = false)
    public Long version;

    /**
     * Default constructor for JPA.
     */
    public DatasourceConfigEntity() {}

    /**
     * Create a new datasource configuration.
     *
     * @param datasourceId unique identifier for the datasource
     * @param apiKey API key for intake service authentication
     * @param s3ConfigJson JSON representation of S3 connection configuration
     */
    public DatasourceConfigEntity(String datasourceId, String apiKey, String s3ConfigJson) {
        this.datasourceId = datasourceId;
        this.apiKey = apiKey;
        this.s3ConfigJson = s3ConfigJson;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = OffsetDateTime.now();
    }

    /**
     * Update the S3 configuration.
     *
     * @param s3ConfigJson new JSON representation of S3 configuration
     */
    public void updateS3Config(String s3ConfigJson) {
        this.s3ConfigJson = s3ConfigJson;
        this.updatedAt = OffsetDateTime.now();
    }

    /**
     * Update the API key.
     *
     * @param apiKey new API key
     */
    public void updateApiKey(String apiKey) {
        this.apiKey = apiKey;
        this.updatedAt = OffsetDateTime.now();
    }
}