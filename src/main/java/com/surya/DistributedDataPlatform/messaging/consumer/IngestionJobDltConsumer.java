package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchPersistenceService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobDltConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(IngestionJobDltConsumer.class);

    private final BatchPersistenceService persistenceService;

    public IngestionJobDltConsumer(
            BatchPersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.ingestion-jobs-dlt}",
            groupId = "ingestion-dlt-handler-group"
    )
    public void handleDeadLetter(IngestionJobEvent event) {

        log.error(
                "Ingestion job {} moved to dead-letter topic. Event ID: {}",
                event.jobId(),
                event.eventId()
        );

        persistenceService.fail(event.jobId());
    }
}