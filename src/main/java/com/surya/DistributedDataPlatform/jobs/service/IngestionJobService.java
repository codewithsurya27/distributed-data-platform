package com.surya.DistributedDataPlatform.jobs.service;

import com.surya.DistributedDataPlatform.common.exception.IngestionJobNotFoundException;
import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.messaging.outbox.service.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class IngestionJobService {

    private static final Logger log =
            LoggerFactory.getLogger(IngestionJobService.class);

    private final IngestionJobRepository ingestionJobRepository;
    private final OutboxService outboxService;

    public IngestionJobService(
            IngestionJobRepository ingestionJobRepository,
            OutboxService outboxService
    ) {
        this.ingestionJobRepository = ingestionJobRepository;
        this.outboxService = outboxService;
    }

    @Transactional
    public IngestionJobResponse create(CreateIngestionJobRequest request) {

        boolean hasSchema = request.targetSchema() != null
                && !request.targetSchema().isBlank();

        boolean hasTable = request.targetTable() != null
                && !request.targetTable().isBlank();

        if (hasSchema != hasTable) {
            throw new IllegalArgumentException(
                    "Target schema and target table must be provided together"
            );
        }

        IngestionJob job = new IngestionJob(
                request.fileName().trim(),
                request.filePath(),
                hasSchema ? request.targetSchema().trim() : null,
                hasTable ? request.targetTable().trim() : null
        );

        IngestionJob savedJob = ingestionJobRepository.save(job);

        // Keep job creation and outbox event creation in one transaction.
        outboxService.createJobEvent(savedJob.getId());

        log.info(
                "Created ingestion job {} for file {} targeting {}.{}",
                savedJob.getId(),
                request.fileName(),
                savedJob.getTargetSchema(),
                savedJob.getTargetTable()
        );

        return IngestionJobResponse.from(savedJob);
    }

    @Transactional(readOnly = true)
    public IngestionJobResponse getById(UUID jobId) {

        IngestionJob job = ingestionJobRepository.findById(jobId)
                .orElseThrow(() -> new IngestionJobNotFoundException(jobId));

        return IngestionJobResponse.from(job);
    }

    @Transactional(readOnly = true)
    public Page<IngestionJobResponse> findAll(Pageable pageable) {

        return ingestionJobRepository.findAll(pageable)
                .map(IngestionJobResponse::from);
    }
}