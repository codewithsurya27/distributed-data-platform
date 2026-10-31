package com.surya.DistributedDataPlatform.messaging.consumer;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ProcessedEventService {

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

        return inserted == 1;
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