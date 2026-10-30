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
    private UUID id;

    @Column(name = "file_name", nullable = false, length = 255)
    private String fileName;

    @Column(name = "file_path", length = 1000)
    private String filePath;

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

    public IngestionJob(String fileName, String filePath) {
        this.id = UUID.randomUUID();
        this.fileName = fileName;
        this.filePath = filePath;
        this.status = IngestionJobStatus.PENDING;
    }

    @PrePersist
    void setCreatedAt() {
        if (createdAt == null) {
            createdAt = OffsetDateTime.now();
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
        this.startedAt = OffsetDateTime.now();
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
        this.status = IngestionJobStatus.FAILED;
        this.completedAt = OffsetDateTime.now();
    }
}
