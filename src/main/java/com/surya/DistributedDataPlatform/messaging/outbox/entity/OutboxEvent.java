package com.surya.DistributedDataPlatform.messaging.outbox.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private OutboxStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "processing_started_at")
    private Instant processingStartedAt;

    @Column(name = "last_error", length = 2000)
    private String lastError;

    protected OutboxEvent() {
    }

    public OutboxEvent(UUID jobId, String eventType) {
        this.id = UUID.randomUUID();
        this.jobId = jobId;
        this.eventType = eventType;
        this.status = OutboxStatus.PENDING;
        this.attempts = 0;
        this.createdAt = Instant.now();
    }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
        this.processingStartedAt = null;
        this.lastError = null;
    }

    public void recordFailure(Exception exception, int maxAttempts) {
        this.attempts++;

        String message = exception.getMessage();

        this.lastError = message == null
                ? exception.getClass().getSimpleName()
                : message.substring(0, Math.min(message.length(), 2000));

        this.processingStartedAt = null;

        if (this.attempts >= maxAttempts) {
            this.status = OutboxStatus.FAILED;
        } else {
            this.status = OutboxStatus.PENDING;
        }
    }

    public void markProcessing() {
        this.status = OutboxStatus.PROCESSING;
        this.processingStartedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getEventType() {
        return eventType;
    }

    public int getAttempts() {
        return attempts;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public Instant getProcessingStartedAt() {
        return processingStartedAt;
    }
}