package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.producer.IngestionJobProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventStateService stateService;
    private final IngestionJobProducer producer;

    private final int batchSize;

    public OutboxPublisher(
            OutboxEventStateService stateService,
            IngestionJobProducer producer,
            @Value("${app.outbox.batch-size:50}") int batchSize) {

        this.stateService = stateService;
        this.producer = producer;
        this.batchSize = batchSize;
    }

    @Scheduled(
            fixedDelayString = "${app.outbox.poll-interval-ms:2000}"
    )
    public void publishPendingEvents() {

        List<OutboxEvent> pendingEvents =
                stateService.claimPendingEvents(batchSize);

        for (OutboxEvent event : pendingEvents) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEvent event) {

        try {
            IngestionJobEvent kafkaEvent = new IngestionJobEvent(
                    event.getId(),
                    event.getJobId(),
                    java.time.Instant.now()
            );

            producer.publish(kafkaEvent)
                    .get(10, TimeUnit.SECONDS);

            stateService.markPublished(event.getId());

            log.info(
                    "Successfully published outbox event {} for job {}",
                    event.getId(),
                    event.getJobId()
            );

        } catch (Exception exception) {

            stateService.recordFailure(
                    event.getId(),
                    exception
            );

            log.error(
                    "Failed to publish outbox event {}. Attempt: {}",
                    event.getId(),
                    event.getAttempts() + 1,
                    exception
            );
        }
    }
}