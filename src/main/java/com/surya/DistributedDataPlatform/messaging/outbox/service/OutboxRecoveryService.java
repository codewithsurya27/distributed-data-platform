
package com.surya.DistributedDataPlatform.messaging.outbox.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxRecoveryService {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxRecoveryService.class);

    private final JdbcTemplate jdbcTemplate;
    private final int timeoutSeconds;

    public OutboxRecoveryService(
            JdbcTemplate jdbcTemplate,
            @Value("${app.outbox.recovery-timeout-seconds:60}")
            int timeoutSeconds) {

        this.jdbcTemplate = jdbcTemplate;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Scheduled(fixedDelayString =
            "${app.outbox.recovery-interval-ms:30000}")
    @Transactional
    public void recoverAbandonedEvents() {

        int recovered = jdbcTemplate.update(
                """
                UPDATE outbox_events
                SET status = 'PENDING',
                    processing_started_at = NULL
                WHERE status = 'PROCESSING'
                  AND processing_started_at IS NOT NULL
                  AND processing_started_at <
                      CURRENT_TIMESTAMP - (? * INTERVAL '1 second')
                """,
                timeoutSeconds
        );

        if (recovered > 0) {
            log.warn(
                    "Recovered {} abandoned outbox events",
                    recovered
            );
        }
    }
}
