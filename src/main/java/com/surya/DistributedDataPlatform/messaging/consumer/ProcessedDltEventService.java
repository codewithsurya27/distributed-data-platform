
package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.processing.service.BatchPersistenceService;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ProcessedDltEventService {

    private final JdbcTemplate jdbcTemplate;
    private final BatchPersistenceService persistenceService;

    public ProcessedDltEventService(
            JdbcTemplate jdbcTemplate,
            BatchPersistenceService persistenceService) {

        this.jdbcTemplate = jdbcTemplate;
        this.persistenceService = persistenceService;
    }

    @Transactional
    public boolean handleDltEvent(UUID eventId, UUID jobId) {

        int inserted = jdbcTemplate.update(
                """
                INSERT INTO processed_dlt_events (
                    event_id,
                    job_id
                )
                VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                jobId
        );

        if (inserted == 0) {
            return false;
        }

        persistenceService.fail(jobId);

        return true;
    }
}
