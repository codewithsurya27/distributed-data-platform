package com.surya.DistributedDataPlatform.jobs.repository;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface IngestionJobRepository extends JpaRepository<IngestionJob, UUID> {
}
