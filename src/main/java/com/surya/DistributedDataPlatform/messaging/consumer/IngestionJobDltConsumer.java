
package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobDltConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(IngestionJobDltConsumer.class);

    private final ProcessedDltEventService processedDltEventService;

    public IngestionJobDltConsumer(
            ProcessedDltEventService processedDltEventService) {
        this.processedDltEventService = processedDltEventService;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.ingestion-jobs-dlt}",
            groupId = "ingestion-dlt-handler-group"
    )
    public void handleDeadLetter(IngestionJobEvent event) {

        boolean handled = processedDltEventService.handleDltEvent(
                event.eventId(),
                event.jobId()
        );

        if (handled) {
            log.error(
                    "DLT event {} handled for ingestion job {}",
                    event.eventId(),
                    event.jobId()
            );
        } else {
            log.info(
                    "Duplicate DLT event {} ignored for job {}",
                    event.eventId(),
                    event.jobId()
            );
        }
    }
}
