package com.surya.DistributedDataPlatform.messaging.outbox.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OutboxMetricsTest {

    private JdbcTemplate jdbcTemplate;
    private SimpleMeterRegistry meterRegistry;
    private OutboxMetrics outboxMetrics;

    @BeforeEach
    void setUp() {

        jdbcTemplate = mock(JdbcTemplate.class);
        meterRegistry = new SimpleMeterRegistry();

        outboxMetrics = new OutboxMetrics(jdbcTemplate);
    }

    @Test
    void registersOutboxMetricsWithDatabaseCounts() {

        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class),
                eq("PENDING")
        )).thenReturn(5L);

        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class),
                eq("PROCESSING")
        )).thenReturn(2L);

        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class),
                eq("FAILED")
        )).thenReturn(1L);

        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class),
                eq("PUBLISHED")
        )).thenReturn(10L);

        outboxMetrics.bindTo(meterRegistry);

        assertEquals(
                5.0,
                meterRegistry.get("outbox.events.pending").gauge().value()
        );

        assertEquals(
                2.0,
                meterRegistry.get("outbox.events.processing").gauge().value()
        );

        assertEquals(
                1.0,
                meterRegistry.get("outbox.events.failed").gauge().value()
        );

        assertEquals(
                10.0,
                meterRegistry.get("outbox.events.published").gauge().value()
        );
    }
}