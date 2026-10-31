package com.surya.DistributedDataPlatform.messaging.outbox.repository;

import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxEvent;
import com.surya.DistributedDataPlatform.messaging.outbox.entity.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(
            OutboxStatus status,
            Pageable pageable
    );
}