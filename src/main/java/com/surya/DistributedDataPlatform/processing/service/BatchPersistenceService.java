package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJobStatus;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;
import com.surya.DistributedDataPlatform.records.repository.CustomerRecordRepository;
import com.surya.DistributedDataPlatform.records.repository.IngestionErrorRepository;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicCsvRow;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicCsvValidationError;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicJdbcBatchWriter;

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
    private final DynamicJdbcBatchWriter dynamicWriter;

    public BatchPersistenceService(
            CustomerRecordRepository recordRepository,
            IngestionErrorRepository errorRepository,
            IngestionJobRepository jobRepository,
            JdbcTemplate jdbcTemplate,
            DynamicJdbcBatchWriter dynamicWriter) {
        this.recordRepository = recordRepository;
        this.errorRepository = errorRepository;
        this.jobRepository = jobRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.dynamicWriter = dynamicWriter;
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


    /** Reset visible counters without deleting already committed dynamic rows. */
    @Transactional
    public void prepareDynamicAttempt(UUID jobId) {
        IngestionJob job = jobRepository.findById(jobId).orElseThrow();
        job.updateProgress(0, 0, 0);
    }

    /**
     * Atomically records row outcomes and inserts new valid rows.
     *
     * Destination uniqueness conflicts are recorded as rejected rows.
     * Infrastructure and other database failures still roll back the
     * complete transaction and are eligible for Kafka retry.
     *
     * @return number of destination-conflict rows newly rejected
     */
    @Transactional
    public long saveDynamicBatch(
            UUID jobId,
            String schema,
            String table,
            DestinationTableMetadata metadata,
            List<DynamicCsvRow> validRows,
            List<DynamicCsvValidationError> rowErrors,
            long totalRows,
            long processedRows,
            long failedRows) {

        java.util.Set<Long> claimedValidRows = new java.util.HashSet<>();

        for (DynamicCsvRow row : validRows) {
            int claimed = jdbcTemplate.update(
                    "INSERT INTO dynamic_ingestion_rows " +
                            "(job_id, row_number, target_schema, target_table, outcome) " +
                            "VALUES (?, ?, ?, ?, 'INSERTED') " +
                            "ON CONFLICT (job_id, row_number) DO NOTHING",
                    jobId, row.rowNumber(), schema, table
            );

            if (claimed == 1) {
                claimedValidRows.add(row.rowNumber());
            }
        }

        List<DynamicCsvRow> rowsToInsert = validRows.stream()
                .filter(row -> claimedValidRows.contains(row.rowNumber()))
                .toList();

        List<DynamicCsvRow> duplicateRows;

        if (rowsToInsert.isEmpty()) {
            duplicateRows = List.of();
        } else {
            duplicateRows = dynamicWriter.insertBatchWithConflictHandling(
                    schema,
                    table,
                    metadata,
                    rowsToInsert
            );
        }

        List<IngestionError> errorsToSave = new java.util.ArrayList<>();

        for (DynamicCsvRow duplicate : duplicateRows) {
            jdbcTemplate.update(
                    "UPDATE dynamic_ingestion_rows " +
                            "SET outcome = 'REJECTED' " +
                            "WHERE job_id = ? AND row_number = ?",
                    jobId, duplicate.rowNumber()
            );

            errorsToSave.add(new IngestionError(
                    jobId,
                    duplicate.rowNumber(),
                    "DUPLICATE_DESTINATION_KEY",
                    "Row conflicts with an existing unique or primary-key " +
                            "constraint in destination " + schema + "." + table
            ));
        }

        java.util.Map<Long, List<DynamicCsvValidationError>> errorsByRow =
                rowErrors.stream().collect(
                        java.util.stream.Collectors.groupingBy(
                                DynamicCsvValidationError::rowNumber
                        )
                );

        for (var entry : errorsByRow.entrySet()) {
            int claimed = jdbcTemplate.update(
                    "INSERT INTO dynamic_ingestion_rows " +
                            "(job_id, row_number, target_schema, target_table, outcome) " +
                            "VALUES (?, ?, ?, ?, 'REJECTED') " +
                            "ON CONFLICT (job_id, row_number) DO NOTHING",
                    jobId, entry.getKey(), schema, table
            );

            if (claimed == 1) {
                for (DynamicCsvValidationError error : entry.getValue()) {
                    errorsToSave.add(new IngestionError(
                            jobId,
                            error.rowNumber(),
                            error.errorCode(),
                            error.columnName() + ": " + error.message()
                    ));
                }
            }
        }

        if (!errorsToSave.isEmpty()) {
            errorRepository.saveAll(errorsToSave);
        }

        IngestionJob job = jobRepository.findById(jobId).orElseThrow();

        job.updateProgress(
                totalRows,
                processedRows,
                failedRows + duplicateRows.size()
        );

        return duplicateRows.size();
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