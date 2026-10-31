
package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxStatus;
import com.surya.DistributedDataPlatform.messaging.outbox.repository.OutboxEventRepository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class OutboxEventStateService {

    private final OutboxEventRepository outboxEventRepository;
    private final JdbcTemplate jdbcTemplate;

    public OutboxEventStateService(
            OutboxEventRepository outboxEventRepository,
            JdbcTemplate jdbcTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public List<OutboxEvent> claimPendingEvents(int batchSize) {

        List<UUID> claimedIds = jdbcTemplate.query(
                """
                WITH selected_events AS (
                    SELECT id
                    FROM outbox_events
                    WHERE status = 'PENDING'
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT ?
                )
                UPDATE outbox_events AS event
                SET status = 'PROCESSING',
                processing_started_at = CURRENT_TIMESTAMP
                FROM selected_events
                WHERE event.id = selected_events.id
                RETURNING event.id
                """,
                (resultSet, rowNumber) ->
                        resultSet.getObject("id", UUID.class),
                batchSize
        );

        if (claimedIds.isEmpty()) {
            return List.of();
        }

        List<OutboxEvent> events =
                outboxEventRepository.findAllById(claimedIds);

        // Preserve the order in which events were claimed.
        List<OutboxEvent> orderedEvents = new ArrayList<>();

        for (UUID id : claimedIds) {
            events.stream()
                    .filter(event -> event.getId().equals(id))
                    .findFirst()
                    .ifPresent(orderedEvents::add);
        }

        return orderedEvents;
    }

    @Transactional
    public void markPublished(UUID eventId) {

        OutboxEvent event = outboxEventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException(
                        "Outbox event not found: " + eventId));

        event.markPublished();
    }

    @Transactional
    public void recordFailure(UUID eventId, Exception exception) {

        OutboxEvent event = outboxEventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalStateException(
                        "Outbox event not found: " + eventId));

        event.recordFailure(exception);

        // Make the event eligible for another publishing attempt.
        jdbcTemplate.update(
                """
                UPDATE outbox_events
                SET status = 'PENDING'
                WHERE id = ?
                  AND status = 'PROCESSING'
                """,
                eventId
        );
    }
}
