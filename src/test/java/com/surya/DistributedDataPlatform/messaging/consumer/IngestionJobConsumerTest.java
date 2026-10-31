
package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.*;

class IngestionJobConsumerTest {

    private BatchProcessingService processingService;
    private ProcessedEventService processedEventService;
    private IngestionJobConsumer consumer;

    @BeforeEach
    void setUp() {
        processingService = mock(BatchProcessingService.class);
        processedEventService = mock(ProcessedEventService.class);

        consumer = new IngestionJobConsumer(
                processingService,
                processedEventService,
                false
        );
    }

    @Test
    void processesEventOnlyOnceWhenDuplicateIsReceived() {

        UUID eventId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();

        IngestionJobEvent event = new IngestionJobEvent(
                eventId,
                jobId,
                Instant.now()
        );

        when(processedEventService.claimEvent(eventId, jobId))
                .thenReturn(true, false);

        // First delivery: event is claimed and processed.
        consumer.consume(event);

        // Second delivery: duplicate event is ignored.
        consumer.consume(event);

        verify(processingService, times(1)).process(jobId);

        verify(processedEventService, times(1))
                .markCompleted(eventId);

        verify(processedEventService, never())
                .releaseEvent(eventId);
    }
}
