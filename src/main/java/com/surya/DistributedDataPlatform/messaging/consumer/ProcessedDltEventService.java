package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.processing.service.BatchPersistenceService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ProcessedDltEventService {

    private static final Logger log =
            LoggerFactory.getLogger(ProcessedDltEventService.class);

    private final JdbcTemplate jdbcTemplate;
    private final BatchPersistenceService persistenceService;
    private final MeterRegistry meterRegistry;

    public ProcessedDltEventService(
            JdbcTemplate jdbcTemplate,
            BatchPersistenceService persistenceService,
            MeterRegistry meterRegistry) {

        this.jdbcTemplate = jdbcTemplate;
        this.persistenceService = persistenceService;
        this.meterRegistry = meterRegistry;
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
            log.debug("DLT event {} for job {} was already handled", eventId, jobId);
            return false;
        }

        persistenceService.fail(jobId);

        meterRegistry.counter("ingestion.jobs.failed").increment();

        return true;
    }
}