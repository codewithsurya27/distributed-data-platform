package com.surya.DistributedDataPlatform.messaging.event;

import java.time.Instant;
import java.util.UUID;

public record IngestionJobEvent(
        UUID eventId,
        UUID jobId,
        Instant createdAt
) {
}