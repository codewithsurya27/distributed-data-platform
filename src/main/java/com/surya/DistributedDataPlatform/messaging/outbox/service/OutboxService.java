package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;

    public OutboxService(OutboxEventRepository outboxEventRepository) {
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void createJobEvent(UUID jobId) {

        OutboxEvent event = new OutboxEvent(
                jobId,
                "INGESTION_JOB_CREATED"
        );

        outboxEventRepository.save(event);
    }
}