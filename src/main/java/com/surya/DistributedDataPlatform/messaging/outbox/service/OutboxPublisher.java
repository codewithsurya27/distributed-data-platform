package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.producer.IngestionJobProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxEventStateService stateService;
    private final IngestionJobProducer producer;
    private final int batchSize;

    public OutboxPublisher(OutboxEventStateService stateService, IngestionJobProducer producer,
                           @Value("${app.outbox.batch-size:50}") int batchSize) {
        this.stateService = stateService;
        this.producer = producer;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:2000}")
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = stateService.claimPendingEvents(batchSize);
        if (!pendingEvents.isEmpty()) {
            log.info("Outbox publisher claimed events for Kafka delivery (count={})", pendingEvents.size());
        }
        for (OutboxEvent event : pendingEvents) publishEvent(event);
    }

    private void publishEvent(OutboxEvent event) {
        MDC.put("jobId", event.getJobId().toString());
        MDC.put("eventId", event.getId().toString());
        try {
            long startedAt = System.nanoTime();
            IngestionJobEvent kafkaEvent = new IngestionJobEvent(event.getId(), event.getJobId(), Instant.now());
            producer.publish(kafkaEvent).get(10, TimeUnit.SECONDS);
            stateService.markPublished(event.getId());
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000;
            log.info("Outbox event successfully delivered to Kafka (durationMs={})", durationMs);
        } catch (Exception exception) {
            stateService.recordFailure(event.getId(), exception);
            log.error("Outbox event delivery to Kafka failed; retry or terminal-failure state is recorded by the outbox state service", exception);
        } finally {
            MDC.remove("jobId");
            MDC.remove("eventId");
        }
    }
}