package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobDltConsumer {
    private static final Logger log = LoggerFactory.getLogger(IngestionJobDltConsumer.class);
    private final ProcessedDltEventService processedDltEventService;

    public IngestionJobDltConsumer(ProcessedDltEventService processedDltEventService) {
        this.processedDltEventService = processedDltEventService;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.ingestion-jobs-dlt}",
            groupId = "ingestion-dlt-handler-group"
    )
    public void handleDeadLetter(IngestionJobEvent event) {
        MDC.put("jobId", event.jobId().toString());
        MDC.put("eventId", event.eventId().toString());
        try {
            log.error("Ingestion event reached the dead-letter topic after Kafka retries were exhausted");
            boolean handled = processedDltEventService.handleDltEvent(event.eventId(), event.jobId());
            if (handled) {
                log.error("Dead-letter event handled; ingestion job marked FAILED");
            } else {
                log.info("Duplicate dead-letter event ignored; it was already handled");
            }
        } finally {
            MDC.remove("jobId");
            MDC.remove("eventId");
        }
    }
}