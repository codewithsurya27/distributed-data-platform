package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;
import com.surya.DistributedDataPlatform.records.repository.CustomerRecordRepository;
import com.surya.DistributedDataPlatform.records.repository.IngestionErrorRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class BatchPersistenceService {

    private final CustomerRecordRepository recordRepository;
    private final IngestionErrorRepository errorRepository;
    private final IngestionJobRepository jobRepository;

    public BatchPersistenceService(
            CustomerRecordRepository recordRepository,
            IngestionErrorRepository errorRepository,
            IngestionJobRepository jobRepository) {
        this.recordRepository = recordRepository;
        this.errorRepository = errorRepository;
        this.jobRepository = jobRepository;
    }

    @Transactional
    public void saveBatch(
            UUID jobId,
            List<CustomerRecord> records,
            List<IngestionError> errors,
            long totalRows,
            long processedRows,
            long failedRows) {

        recordRepository.saveAll(records);
        errorRepository.saveAll(errors);

        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        job.updateProgress(totalRows, processedRows, failedRows);
    }

    @Transactional
    public void start(UUID jobId) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();
        job.startProcessing();
    }

    @Transactional
    public void finish(UUID jobId, boolean hasErrors) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();
        job.complete(hasErrors);
    }

    @Transactional
    public void fail(UUID jobId) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();
        job.fail();
    }
}