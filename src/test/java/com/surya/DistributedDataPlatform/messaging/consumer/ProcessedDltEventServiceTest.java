package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.processing.service.BatchPersistenceService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProcessedDltEventServiceTest {

    private JdbcTemplate jdbcTemplate;
    private BatchPersistenceService persistenceService;
    private SimpleMeterRegistry meterRegistry;
    private ProcessedDltEventService service;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        persistenceService = mock(BatchPersistenceService.class);
        meterRegistry = new SimpleMeterRegistry();

        service = new ProcessedDltEventService(
                jdbcTemplate,
                persistenceService,
                meterRegistry
        );
    }

    @Test
    void handlesNewDltEventAndFailsJob() {

        UUID eventId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();

        when(jdbcTemplate.update(
                anyString(),
                eq(eventId),
                eq(jobId)
        )).thenReturn(1);

        boolean handled = service.handleDltEvent(eventId, jobId);

        assertTrue(handled);
        verify(persistenceService).fail(jobId);

        assertEquals(
                1.0,
                meterRegistry.counter("ingestion.jobs.failed").count()
        );
    }

    @Test
    void ignoresDuplicateDltEvent() {

        UUID eventId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();

        when(jdbcTemplate.update(
                anyString(),
                eq(eventId),
                eq(jobId)
        )).thenReturn(0);

        boolean handled = service.handleDltEvent(eventId, jobId);

        assertFalse(handled);

        verify(persistenceService, never()).fail(any());

        assertEquals(
                0.0,
                meterRegistry.counter("ingestion.jobs.failed").count()
        );
    }
}