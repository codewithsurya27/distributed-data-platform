package com.surya.DistributedDataPlatform.records.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "ingestion_errors")
public class IngestionError {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "row_number", nullable = false)
    private long rowNumber;

    @Column(name = "error_code", nullable = false, length = 100)
    private String errorCode;

    @Column(name = "error_message", nullable = false, length = 1000)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected IngestionError() {
    }

    public IngestionError(
            UUID jobId, long rowNumber, String errorCode, String errorMessage) {
        this.id = UUID.randomUUID();
        this.jobId = jobId;
        this.rowNumber = rowNumber;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.createdAt = OffsetDateTime.now();
    }

    public UUID getId() { return id; }
    public UUID getJobId() { return jobId; }
    public long getRowNumber() { return rowNumber; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}