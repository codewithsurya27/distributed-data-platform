package com.surya.DistributedDataPlatform.jobs.service;

import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.messaging.outbox.service.OutboxService;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.surya.DistributedDataPlatform.common.exception.IngestionJobNotFoundException;

import java.util.UUID;

@Service
public class IngestionJobService {

    private final IngestionJobRepository ingestionJobRepository;
    private final OutboxService outboxService;

    public IngestionJobService(
            IngestionJobRepository ingestionJobRepository,
            OutboxService outboxService
    ) {
        this.ingestionJobRepository = ingestionJobRepository;
        this.outboxService = outboxService;
    }

    // Create job and outbox event in the SAME transaction.
    @Transactional
    public IngestionJobResponse create(CreateIngestionJobRequest request) {

        IngestionJob job = new IngestionJob(
                request.fileName().trim(),
                request.filePath()
        );

        // Save ingestion job.
        IngestionJob savedJob = ingestionJobRepository.save(job);

        // Save outbox event in the same transaction.
        outboxService.createJobEvent(savedJob.getId());

        // Return DTO.
        return IngestionJobResponse.from(savedJob);
    }

    // Retrieve a job by ID.
    @Transactional(readOnly = true)
    public IngestionJobResponse getById(UUID jobId) {

        IngestionJob job = ingestionJobRepository.findById(jobId)
                .orElseThrow(() -> new IngestionJobNotFoundException(jobId));

        return IngestionJobResponse.from(job);
    }

    // Retrieve paginated jobs.
    @Transactional(readOnly = true)
    public Page<IngestionJobResponse> findAll(Pageable pageable) {

        return ingestionJobRepository.findAll(pageable)
                .map(IngestionJobResponse::from);
    }
}
