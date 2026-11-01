package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobConsumer {

    private static final Logger log = LoggerFactory.getLogger(IngestionJobConsumer.class);
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
        MDC.put("jobId", event.jobId().toString());
        MDC.put("eventId", event.eventId().toString());
        try {
            log.info("Kafka ingestion event received");
            boolean claimed = processedEventService.claimEvent(event.eventId(), event.jobId());
            if (!claimed) {
                log.info("Duplicate ingestion event ignored; it has already been claimed or processed");
                return;
            }

            log.info("Ingestion event claimed; processing will begin");
            try {
                if (testFailEnabled) {
                    log.warn("Intentional test failure is enabled; throwing exception to verify retry and DLT handling");
                    throw new RuntimeException("Intentional test failure for DLT verification");
                }
                processingService.process(event.jobId());
                processedEventService.markCompleted(event.eventId());
                log.info("Kafka ingestion event processing completed");
            } catch (RuntimeException exception) {
                log.warn("Ingestion event processing failed; releasing event claim so Kafka retry can occur", exception);
                processedEventService.releaseEvent(event.eventId());
                throw exception;
            }
        } finally {
            MDC.remove("jobId");
            MDC.remove("eventId");
        }
    }
}