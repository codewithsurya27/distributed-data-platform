package com.surya.DistributedDataPlatform.records.repository;

import com.surya.DistributedDataPlatform.records.entity.IngestionError;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface IngestionErrorRepository
        extends JpaRepository<IngestionError, UUID> {

    List<IngestionError> findByJobIdOrderByRowNumberAsc(UUID jobId);
}