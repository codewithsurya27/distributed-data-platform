package com.surya.DistributedDataPlatform.jobs.service;

import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.messaging.outbox.service.OutboxService;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.surya.DistributedDataPlatform.common.exception.IngestionJobNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@Service
public class IngestionJobService {

    private static final Logger log = LoggerFactory.getLogger(IngestionJobService.class);

    private final IngestionJobRepository ingestionJobRepository;
    private final OutboxService outboxService;
    private final MeterRegistry meterRegistry;

    public IngestionJobService(
            IngestionJobRepository ingestionJobRepository,
            OutboxService outboxService,
            MeterRegistry meterRegistry
    ) {
        this.ingestionJobRepository = ingestionJobRepository;
        this.outboxService = outboxService;
        this.meterRegistry = meterRegistry;
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
        meterRegistry.counter("ingestion.jobs.created").increment();

        log.info("Created ingestion job {} for file {}", savedJob.getId(), request.fileName());

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
