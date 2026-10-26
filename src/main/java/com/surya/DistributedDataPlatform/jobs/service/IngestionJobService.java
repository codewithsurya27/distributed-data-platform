package com.surya.DistributedDataPlatform.jobs.service;

import com.surya.DistributedDataPlatform.common.exception.IngestionJobNotFoundException;
import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Transactional(readOnly = true)
public class IngestionJobService {

    private final IngestionJobRepository repository;

    public IngestionJobService(IngestionJobRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public IngestionJobResponse create(CreateIngestionJobRequest request) {
        IngestionJob job = new IngestionJob(request.fileName().trim(), request.filePath());
        return IngestionJobResponse.from(repository.save(job));
    }

    public IngestionJobResponse getById(UUID id) {
        return repository.findById(id)
                .map(IngestionJobResponse::from)
                .orElseThrow(() -> new IngestionJobNotFoundException(id));
    }

    public Page<IngestionJobResponse> findAll(Pageable pageable) {
        return repository.findAll(pageable).map(IngestionJobResponse::from);
    }
}
