package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.processing.parser.CsvRecordParser;
import com.surya.DistributedDataPlatform.processing.validator.CustomerRecordValidator;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class BatchProcessingService {

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
        IngestionJob job = jobRepository.findById(jobId)
                .orElseThrow();

        Path file = Path.of(job.getFilePath());

        if (!Files.isRegularFile(file)) {
            persistenceService.fail(jobId);
            return;
        }

        persistenceService.start(jobId);

        long total = 0;
        long failed = 0;
        long processed = 0;

        List<CustomerRecord> validBatch = new ArrayList<>();
        List<IngestionError> errorBatch = new ArrayList<>();

        try (CSVParser csvParser = parser.parse(file)) {
            for (CSVRecord row : csvParser) {
                total++;

                String validationError = validator.validate(row);

                if (validationError != null) {
                    failed++;
                    errorBatch.add(new IngestionError(
                            jobId, row.getRecordNumber() + 1,
                            "VALIDATION_ERROR", validationError));
                } else {
                    validBatch.add(new CustomerRecord(
                            jobId,
                            validator.value(row, "customerId"),
                            validator.value(row, "name"),
                            validator.value(row, "email")));
                }

                processed++;

                if (processed % batchSize == 0) {
                    persistenceService.saveBatch(
                            jobId, validBatch, errorBatch,
                            total, processed, failed);
                    validBatch = new ArrayList<>();
                    errorBatch = new ArrayList<>();
                }
            }

            if (!validBatch.isEmpty() || !errorBatch.isEmpty()) {
                persistenceService.saveBatch(
                        jobId, validBatch, errorBatch,
                        total, processed, failed);
            }

            persistenceService.finish(jobId, failed > 0);

        } catch (IOException | RuntimeException ex) {
            persistenceService.fail(jobId);
        }
    }
}