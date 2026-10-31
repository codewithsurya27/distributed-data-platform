package com.surya.DistributedDataPlatform.messaging.outbox.service;

import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxStatus;
import com.surya.DistributedDataPlatform.messaging.outbox.repository.OutboxEventRepository;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class OutboxEventStateService {

    private final OutboxEventRepository outboxEventRepository;

    public OutboxEventStateService(
            OutboxEventRepository outboxEventRepository) {
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional(readOnly = true)
    public List<OutboxEvent> findPendingEvents(int batchSize) {

        return outboxEventRepository.findByStatusOrderByCreatedAtAsc(
                OutboxStatus.PENDING,
                PageRequest.of(0, batchSize)
        );
    }

    @Transactional
    public void markPublished(UUID eventId) {

        OutboxEvent event = outboxEventRepository.findById(eventId)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Outbox event not found: " + eventId
                        ));

        event.markPublished();
    }

    @Transactional
    public void recordFailure(UUID eventId, Exception exception) {

        OutboxEvent event = outboxEventRepository.findById(eventId)
                .orElseThrow(() ->
                        new IllegalStateException(
                                "Outbox event not found: " + eventId
                        ));

        event.recordFailure(exception);
    }
}