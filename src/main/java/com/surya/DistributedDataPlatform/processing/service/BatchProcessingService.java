package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.processing.parser.CsvRecordParser;
import com.surya.DistributedDataPlatform.processing.validator.CustomerRecordValidator;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class BatchProcessingService {
    private static final Logger log = LoggerFactory.getLogger(BatchProcessingService.class);
    private final IngestionJobRepository jobRepository;
    private final CsvRecordParser parser;
    private final CustomerRecordValidator validator;
    private final BatchPersistenceService persistenceService;
    private final int batchSize;

    public BatchProcessingService(
            IngestionJobRepository jobRepository,
            CsvRecordParser parser,
            CustomerRecordValidator validator,
            BatchPersistenceService persistenceService,
            @Value("${app.processing.batch-size:500}") int batchSize) {
        this.jobRepository = jobRepository;
        this.parser = parser;
        this.validator = validator;
        this.persistenceService = persistenceService;
        this.batchSize = batchSize;
    }

    public void process(UUID jobId) {
        String previousJobId = MDC.get("jobId");
        MDC.put("jobId", jobId.toString());
        long startedAt = System.nanoTime();
        try {
            processClaimedJob(jobId, startedAt);
        } finally {
            if (previousJobId == null) MDC.remove("jobId");
            else MDC.put("jobId", previousJobId);
        }
    }

    private void processClaimedJob(UUID jobId, long startedAt) {
        boolean claimed = persistenceService.claimJob(jobId);

        if (!claimed) {
            log.debug("Job processing skipped because the job is already claimed or is not pending");
            return;
        }

        log.info("CSV ingestion processing started (batchSize={})", batchSize);
        try {
            IngestionJob job = jobRepository.findById(jobId)
                    .orElseThrow();

            if (job.getFilePath() == null || job.getFilePath().isBlank()) {
                throw new IllegalStateException(
                        "No file path configured for job: " + jobId);
            }

            Path file = Path.of(job.getFilePath());

            if (!Files.isRegularFile(file)) {
                throw new IllegalStateException(
                        "CSV file does not exist: " + file);
            }

            // Remove partial data from any previous attempt.
            persistenceService.prepareAttempt(jobId);
            long totalRowsRead = 0;
            long failedRows = 0;
            long validRows = 0;
            long batchNumber = 0;
            List<CustomerRecord> validBatch = new ArrayList<>();
            List<IngestionError> errorBatch = new ArrayList<>();

            // Track customer IDs across the entire CSV, including batches.
            Set<String> seenCustomerIds = new HashSet<>();

            try (CSVParser csvParser = parser.parse(file)) {

                for (CSVRecord row : csvParser) {
                    totalRowsRead++;
                    String validationError = validator.validate(row);

                    if (validationError != null) {
                        failedRows++;
                        errorBatch.add(new IngestionError(jobId, row.getRecordNumber() + 1,
                                "VALIDATION_ERROR", validationError));
                    } else {
                        String customerId = validator.value(row, "customerId");
                        if (!seenCustomerIds.add(customerId)) {
                            failedRows++;
                            errorBatch.add(new IngestionError(jobId, row.getRecordNumber() + 1,
                                    "DUPLICATE_CUSTOMER_ID", "Duplicate customerId found: " + customerId));
                        } else {
                            validRows++;
                            validBatch.add(new CustomerRecord(jobId, customerId,
                                    validator.value(row, "name"), validator.value(row, "email")));
                        }
                    }

                    if (totalRowsRead % batchSize == 0) {
                        batchNumber++;
                        persistAndLogBatch(jobId, batchNumber, validBatch, errorBatch,
                                totalRowsRead, validRows, failedRows, startedAt);
                        validBatch = new ArrayList<>();
                        errorBatch = new ArrayList<>();
                    }
                }

                if (!validBatch.isEmpty() || !errorBatch.isEmpty()) {
                    batchNumber++;
                    persistAndLogBatch(jobId, batchNumber, validBatch, errorBatch,
                            totalRowsRead, validRows, failedRows, startedAt);
                }
            }

            persistenceService.finish(jobId, failedRows > 0);
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000;
            log.info("CSV ingestion processing completed: status={}, totalRowsRead={}, validRows={}, rejectedRows={}, batches={}, durationMs={}",
                    failedRows > 0 ? "COMPLETED_WITH_ERRORS" : "COMPLETED",
                    totalRowsRead, validRows, failedRows, batchNumber, durationMs);
        } catch (IOException | RuntimeException exception) {
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000;
            log.error("CSV ingestion processing failed; job will be released for retry (durationMs={})",
                    durationMs, exception);
            try {
                persistenceService.releaseForRetry(jobId);
            } catch (RuntimeException releaseException) {
                log.error("Failed to release ingestion job for retry", releaseException);
                exception.addSuppressed(releaseException);
            }
            throw new IllegalStateException("Failed to process ingestion job: " + jobId, exception);
        }
    }

    private void persistAndLogBatch(UUID jobId, long batchNumber,
                                    List<CustomerRecord> validBatch, List<IngestionError> errorBatch,
                                    long totalRowsRead, long validRows, long failedRows, long startedAt) {
        persistenceService.saveBatch(jobId, validBatch, errorBatch,
                totalRowsRead, totalRowsRead, failedRows);
        long durationMs = (System.nanoTime() - startedAt) / 1_000_000;
        log.info("CSV batch persisted: batchNumber={}, rowsReadSoFar={}, validRowsSoFar={}, rejectedRowsSoFar={}, recordsInBatch={}, errorsInBatch={}, elapsedMs={}",
                batchNumber, totalRowsRead, validRows, failedRows,
                validBatch.size(), errorBatch.size(), durationMs);
    }
}
