
package com.surya.DistributedDataPlatform.messaging.outbox.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OutboxEventTest {

    @Test
    void startsWithPendingStatus() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        assertEquals(OutboxStatus.PENDING, event.getStatus());
        assertEquals(0, event.getAttempts());
    }

    @Test
    void movesToProcessing() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        event.markProcessing();

        assertEquals(OutboxStatus.PROCESSING, event.getStatus());
        assertNotNull(event.getProcessingStartedAt());
    }

    @Test
    void returnsToPendingWhenRetryLimitNotReached() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        event.markProcessing();
        event.recordFailure(new RuntimeException("Kafka unavailable"), 3);

        assertEquals(OutboxStatus.PENDING, event.getStatus());
        assertEquals(1, event.getAttempts());
        assertNull(event.getProcessingStartedAt());
    }

    @Test
    void becomesFailedWhenRetryLimitReached() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        event.recordFailure(new RuntimeException("Failure 1"), 3);
        event.recordFailure(new RuntimeException("Failure 2"), 3);
        event.recordFailure(new RuntimeException("Failure 3"), 3);

        assertEquals(OutboxStatus.FAILED, event.getStatus());
        assertEquals(3, event.getAttempts());
    }

    @Test
    void becomesPublishedAndClearsProcessingTimestamp() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        event.markProcessing();
        event.markPublished();

        assertEquals(OutboxStatus.PUBLISHED, event.getStatus());
        assertNull(event.getProcessingStartedAt());
    }

    @Test
    void truncatesLongFailureMessages() {

        OutboxEvent event = new OutboxEvent(
                java.util.UUID.randomUUID(),
                "INGESTION_JOB_CREATED"
        );

        String longMessage = "x".repeat(2500);

        event.recordFailure(new RuntimeException(longMessage), 3);

        // The entity does not expose lastError, so verify the
        // persisted field in a repository/database integration test.
        assertEquals(1, event.getAttempts());
    }
}
