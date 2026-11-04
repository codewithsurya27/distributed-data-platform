
package com.surya.DistributedDataPlatform.jobs.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "ingestion_jobs")
public class IngestionJob {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "file_name", nullable = false, length = 255)
    private String fileName;

    @Column(name = "file_path", length = 1000)
    private String filePath;

    @Column(name = "target_schema", length = 63)
    private String targetSchema;

    @Column(name = "target_table", length = 63)
    private String targetTable;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 40)
    private IngestionJobStatus status;

    @Column(name = "total_rows", nullable = false)
    private long totalRows;

    @Column(name = "processed_rows", nullable = false)
    private long processedRows;

    @Column(name = "failed_rows", nullable = false)
    private long failedRows;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "started_at")
    private OffsetDateTime startedAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    protected IngestionJob() {
    }

    // Retained for existing code and tests.
    public IngestionJob(String fileName, String filePath) {
        this(fileName, filePath, null, null);
    }

    public IngestionJob(
            String fileName,
            String filePath,
            String targetSchema,
            String targetTable
    ) {
        this(UUID.randomUUID(), fileName, filePath, targetSchema, targetTable);
    }

    // Constructor allowing an explicitly supplied job ID.
    public IngestionJob(
            UUID id,
            String fileName,
            String filePath,
            String targetSchema,
            String targetTable
    ) {
        this.id = id != null ? id : UUID.randomUUID();
        this.fileName = fileName;
        this.filePath = filePath;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.status = IngestionJobStatus.PENDING;
        this.totalRows = 0;
        this.processedRows = 0;
        this.failedRows = 0;
    }

    @PrePersist
    void setCreatedAt() {
        if (createdAt == null) {
            createdAt = OffsetDateTime.now();
        }

        if (id == null) {
            id = UUID.randomUUID();
        }

        if (status == null) {
            status = IngestionJobStatus.PENDING;
        }
    }

    public UUID getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public IngestionJobStatus getStatus() {
        return status;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getProcessedRows() {
        return processedRows;
    }

    public long getFailedRows() {
        return failedRows;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getStartedAt() {
        return startedAt;
    }

    public OffsetDateTime getCompletedAt() {
        return completedAt;
    }

    public void startProcessing() {
        this.status = IngestionJobStatus.PROCESSING;

        if (this.startedAt == null) {
            this.startedAt = OffsetDateTime.now();
        }

        this.completedAt = null;
    }

    public void updateProgress(long total, long processed, long failed) {
        this.totalRows = total;
        this.processedRows = processed;
        this.failedRows = failed;
    }

    public void complete(boolean hasErrors) {
        this.status = hasErrors
                ? IngestionJobStatus.COMPLETED_WITH_ERRORS
                : IngestionJobStatus.COMPLETED;

        this.completedAt = OffsetDateTime.now();
    }

    public void fail() {
        if (this.status == IngestionJobStatus.COMPLETED) {
            return;
        }

        this.status = IngestionJobStatus.FAILED;
        this.completedAt = OffsetDateTime.now();
    }

    public void resetForRetry() {
        this.status = IngestionJobStatus.PENDING;
        this.startedAt = null;
        this.completedAt = null;
    }
}
