package com.surya.DistributedDataPlatform.messaging.outbox.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics implements MeterBinder {

    private final JdbcTemplate jdbcTemplate;

    public OutboxMetrics(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void bindTo(MeterRegistry registry) {

        registerGauge(
                registry,
                "outbox.events.pending",
                "Number of outbox events waiting to be published",
                "PENDING"
        );

        registerGauge(
                registry,
                "outbox.events.processing",
                "Number of outbox events currently being processed",
                "PROCESSING"
        );

        registerGauge(
                registry,
                "outbox.events.failed",
                "Number of outbox events that exhausted retries",
                "FAILED"
        );

        registerGauge(
                registry,
                "outbox.events.published",
                "Number of successfully published outbox events",
                "PUBLISHED"
        );
    }

    private void registerGauge(
            MeterRegistry registry,
            String metricName,
            String description,
            String status) {

        Gauge.builder(
                        metricName,
                        jdbcTemplate,
                        jdbc -> countByStatus(jdbc, status)
                )
                .description(description)
                .register(registry);
    }

    private double countByStatus(
            JdbcTemplate jdbcTemplate,
            String status) {

        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE status = ?",
                Long.class,
                status
        );

        return count == null ? 0.0 : count;
    }
}