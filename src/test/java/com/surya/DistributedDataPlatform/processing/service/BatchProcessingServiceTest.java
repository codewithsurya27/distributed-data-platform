
package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.processing.parser.CsvRecordParser;
import com.surya.DistributedDataPlatform.processing.validator.CustomerRecordValidator;
import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import com.surya.DistributedDataPlatform.records.entity.IngestionError;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BatchProcessingServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void rejectsDuplicateCustomerIdsAndSavesValidRecords() throws Exception {

        Path csv = tempDir.resolve("customers.csv");

        Files.writeString(csv, """
                customerId,name,email
                C001,Alice,alice@example.com
                C002,Bob,bob@example.com
                C001,Duplicate,duplicate@example.com
                """);

        UUID jobId = UUID.randomUUID();

        IngestionJob job = new IngestionJob(
                "customers.csv",
                csv.toString()
        );

        IngestionJobRepository jobRepository =
                mock(IngestionJobRepository.class);

        BatchPersistenceService persistenceService =
                mock(BatchPersistenceService.class);

        when(persistenceService.claimJob(jobId)).thenReturn(true);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        CsvRecordParser parser = new CsvRecordParser();
        CustomerRecordValidator validator = new CustomerRecordValidator();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();

        BatchProcessingService service = new BatchProcessingService(
                jobRepository,
                parser,
                validator,
                persistenceService,
                meterRegistry,
                2
        );

        service.process(jobId);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CustomerRecord>> recordsCaptor =
                ArgumentCaptor.forClass(List.class);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<IngestionError>> errorsCaptor =
                ArgumentCaptor.forClass(List.class);

        verify(persistenceService, times(2)).saveBatch(
                eq(jobId),
                recordsCaptor.capture(),
                errorsCaptor.capture(),
                anyLong(),
                anyLong(),
                anyLong()
        );

        List<CustomerRecord> allRecords = recordsCaptor.getAllValues()
                .stream()
                .flatMap(List::stream)
                .toList();

        List<IngestionError> allErrors = errorsCaptor.getAllValues()
                .stream()
                .flatMap(List::stream)
                .toList();

        assertEquals(2, allRecords.size());

        assertEquals(
                List.of("C001", "C002"),
                allRecords.stream()
                        .map(CustomerRecord::getCustomerId)
                        .toList()
        );

        assertEquals(1, allErrors.size());

        assertEquals(
                "DUPLICATE_CUSTOMER_ID",
                allErrors.get(0).getErrorCode()
        );

        verify(persistenceService).finish(jobId, true);
    }

    @Test
    void doesNotProcessJobWhenClaimFails() {

        UUID jobId = UUID.randomUUID();

        IngestionJobRepository jobRepository =
                mock(IngestionJobRepository.class);

        BatchPersistenceService persistenceService =
                mock(BatchPersistenceService.class);

        when(persistenceService.claimJob(jobId)).thenReturn(false);

        CsvRecordParser parser = new CsvRecordParser();
        CustomerRecordValidator validator = new CustomerRecordValidator();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();

        BatchProcessingService service = new BatchProcessingService(
                jobRepository,
                parser,
                validator,
                persistenceService,
                meterRegistry,
                2
        );

        service.process(jobId);

        verifyNoInteractions(jobRepository);
        verify(persistenceService, never()).prepareAttempt(jobId);
        verify(persistenceService, never()).finish(any(), anyBoolean());
    }
}
