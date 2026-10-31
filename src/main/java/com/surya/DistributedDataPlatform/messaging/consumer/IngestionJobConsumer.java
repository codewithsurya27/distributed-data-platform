package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobConsumer {

    private final BatchProcessingService processingService;
    private final ProcessedEventService processedEventService;

    public IngestionJobConsumer(
            BatchProcessingService processingService,
            ProcessedEventService processedEventService) {

        this.processingService = processingService;
        this.processedEventService = processedEventService;
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
            processingService.process(event.jobId());

            processedEventService.markCompleted(event.eventId());

        } catch (RuntimeException exception) {

            processedEventService.releaseEvent(event.eventId());

            throw exception;
        }
    }
}