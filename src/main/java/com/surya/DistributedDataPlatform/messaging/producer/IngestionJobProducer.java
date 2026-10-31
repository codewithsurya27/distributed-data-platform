package com.surya.DistributedDataPlatform.messaging.producer;

import com.surya.DistributedDataPlatform.messaging.event.IngestionJobEvent;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class IngestionJobProducer {

    private final KafkaTemplate<String, IngestionJobEvent> kafkaTemplate;
    private final String topic;

    public IngestionJobProducer(
            KafkaTemplate<String, IngestionJobEvent> kafkaTemplate,
            @Value("${app.kafka.topic.ingestion-jobs}") String topic) {

        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, IngestionJobEvent>> publish(
            IngestionJobEvent event) {

        return kafkaTemplate.send(
                topic,
                event.jobId().toString(),
                event
        );
    }
}