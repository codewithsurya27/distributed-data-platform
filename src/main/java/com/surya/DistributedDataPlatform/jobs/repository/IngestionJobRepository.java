package com.surya.DistributedDataPlatform.jobs.repository;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface IngestionJobRepository extends JpaRepository<IngestionJob, UUID> {
    List<IngestionJob> findByStatusOrderByCreatedAtAsc(
            IngestionJobStatus status);

    @Modifying
    @Query("""
        UPDATE IngestionJob j
        SET j.status = :processing,
            j.startedAt = :startedAt
        WHERE j.id = :jobId
          AND j.status = :pending
        """)
    int claimPendingJob(
            @Param("jobId") UUID jobId,
            @Param("pending") IngestionJobStatus pending,
            @Param("processing") IngestionJobStatus processing,
            @Param("startedAt") OffsetDateTime startedAt
    );
}
