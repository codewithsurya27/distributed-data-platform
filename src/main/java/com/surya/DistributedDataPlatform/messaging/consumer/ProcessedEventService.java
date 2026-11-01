package com.surya.DistributedDataPlatform.messaging.consumer;

import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ProcessedEventService {

    private static final Logger log = LoggerFactory.getLogger(ProcessedEventService.class);

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean claimEvent(UUID eventId, UUID jobId) {

        int inserted = jdbcTemplate.update("""
                INSERT INTO processed_events (
                    event_id,
                    job_id,
                    status
                )
                VALUES (?, ?, 'IN_PROGRESS')
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                jobId
        );

        boolean claimed = inserted == 1;
        if (!claimed) {
            log.debug("Event {} for job {} was already claimed", eventId, jobId);
        }
        return claimed;
    }

    @Transactional
    public void markCompleted(UUID eventId) {

        jdbcTemplate.update("""
                UPDATE processed_events
                SET status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                  AND status = 'IN_PROGRESS'
                """,
                eventId
        );
    }

    @Transactional
    public void releaseEvent(UUID eventId) {

        jdbcTemplate.update("""
                DELETE FROM processed_events
                WHERE event_id = ?
                  AND status = 'IN_PROGRESS'
                """,
                eventId
        );
    }
}
