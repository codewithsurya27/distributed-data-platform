package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJobStatus;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;
import com.surya.DistributedDataPlatform.records.repository.CustomerRecordRepository;
import com.surya.DistributedDataPlatform.records.repository.IngestionErrorRepository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class BatchPersistenceService {

    private final CustomerRecordRepository recordRepository;
    private final IngestionErrorRepository errorRepository;
    private final IngestionJobRepository jobRepository;
    private final JdbcTemplate jdbcTemplate;

    public BatchPersistenceService(
            CustomerRecordRepository recordRepository,
            IngestionErrorRepository errorRepository,
            IngestionJobRepository jobRepository,
            JdbcTemplate jdbcTemplate) {
        this.recordRepository = recordRepository;
        this.errorRepository = errorRepository;
        this.jobRepository = jobRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean claimJob(UUID jobId) {
        int claimed = jobRepository.claimPendingJob(
                jobId,
                IngestionJobStatus.PENDING,
                IngestionJobStatus.PROCESSING,
                OffsetDateTime.now()
        );

        return claimed == 1;
    }

    @Transactional
    public void prepareAttempt(UUID jobId) {
        jdbcTemplate.update(
                "DELETE FROM ingestion_errors WHERE job_id = ?",
                jobId
        );

        jdbcTemplate.update(
                "DELETE FROM customer_records WHERE job_id = ?",
                jobId
        );

        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        job.updateProgress(0, 0, 0);
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
    public void finish(UUID jobId, boolean hasErrors) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        job.complete(hasErrors);
    }

    @Transactional
    public void releaseForRetry(UUID jobId) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        job.resetForRetry();
    }

    @Transactional
    public void fail(UUID jobId) {
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        job.fail();
    }
}