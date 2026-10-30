package com.surya.DistributedDataPlatform.records.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "customer_records",
        indexes = @Index(name = "idx_customer_records_job_id", columnList = "job_id")
)
public class CustomerRecord {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(nullable = false, length = 255)
    private String name;

    @Column(nullable = false, length = 320)
    private String email;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected CustomerRecord() {
    }

    public CustomerRecord(
            UUID jobId, String customerId, String name, String email) {
        this.id = UUID.randomUUID();
        this.jobId = jobId;
        this.customerId = customerId;
        this.name = name;
        this.email = email;
        this.createdAt = OffsetDateTime.now();
    }

    public UUID getId() { return id; }
    public UUID getJobId() { return jobId; }
    public String getCustomerId() { return customerId; }
    public String getName() { return name; }
    public String getEmail() { return email; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}