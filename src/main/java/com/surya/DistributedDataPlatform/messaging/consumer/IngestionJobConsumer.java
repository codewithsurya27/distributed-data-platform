package com.surya.DistributedDataPlatform.messaging.consumer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class IngestionJobConsumer {

    private final BatchProcessingService processingService;
    private final boolean testFailEnabled;

    public IngestionJobConsumer(
            BatchProcessingService processingService,
            @Value("${app.kafka.test-fail-enabled:false}")
            boolean testFailEnabled) {

        this.processingService = processingService;
        this.testFailEnabled = testFailEnabled;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.ingestion-jobs}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(IngestionJobEvent event) {
        System.out.println(
                "Kafka test failure enabled: " + testFailEnabled
        );

        if (testFailEnabled) {
            throw new IllegalStateException(
                    "TEST FAILURE: Simulating Kafka consumer failure"
            );
        }

        processingService.process(event.jobId());
    }
}