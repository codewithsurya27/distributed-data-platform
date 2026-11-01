
package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxStatus;
import com.surya.DistributedDataPlatform.messaging.outbox.repository.OutboxEventRepository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class OutboxEventStateService {

    private final OutboxEventRepository outboxEventRepository;
    private final JdbcTemplate jdbcTemplate;
    private final int maxAttempts;

    private static final Logger log = LoggerFactory.getLogger(OutboxEventStateService.class);

    public OutboxEventStateService(
            OutboxEventRepository outboxEventRepository,
            JdbcTemplate jdbcTemplate,
            @Value("${app.outbox.max-attempts:3}") int maxAttempts) {

        this.outboxEventRepository = outboxEventRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.maxAttempts = maxAttempts;
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

        log.debug("Claimed {} outbox events", claimedIds.size());

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

        event.recordFailure(exception, maxAttempts);

        if (event.getStatus() == OutboxStatus.FAILED) {
            log.error(
                    "Outbox event {} permanently failed after {} attempts",
                    eventId,
                    event.getAttempts()
            );
        } else {
            log.warn(
                    "Outbox event {} failed. Attempt {}/{}. It will be retried.",
                    eventId,
                    event.getAttempts(),
                    maxAttempts
            );
        }
    }
}
