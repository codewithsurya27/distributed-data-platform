
package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobConsumer {

    private final BatchProcessingService processingService;
    private final ProcessedEventService processedEventService;
    private final boolean testFailEnabled;

    public IngestionJobConsumer(
            BatchProcessingService processingService,
            ProcessedEventService processedEventService,
            @Value("${app.kafka.test-fail-enabled:false}") boolean testFailEnabled) {

        this.processingService = processingService;
        this.processedEventService = processedEventService;
        this.testFailEnabled = testFailEnabled;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.ingestion-jobs}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(IngestionJobEvent event) {

        boolean claimed = processedEventService.claimEvent(
                event.eventId(),
                event.jobId()
        );

        if (!claimed) {
            return;
        }

        try {
            if (testFailEnabled) {
                throw new RuntimeException("Intentional test failure for DLT verification");
            }

            processingService.process(event.jobId());

            processedEventService.markCompleted(event.eventId());

        } catch (RuntimeException exception) {

            processedEventService.releaseEvent(event.eventId());

            throw exception;
        }
    }
}
