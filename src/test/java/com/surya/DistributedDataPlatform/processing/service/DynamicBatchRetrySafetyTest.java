
package com.surya.DistributedDataPlatform.processing.service;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicCsvRow;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicCsvValidationError;
import com.surya.DistributedDataPlatform.processing.dynamic.DynamicJdbcBatchWriter;
import com.surya.DistributedDataPlatform.records.repository.CustomerRecordRepository;
import com.surya.DistributedDataPlatform.records.repository.IngestionErrorRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DynamicBatchRetrySafetyTest {

    private CustomerRecordRepository recordRepository;
    private IngestionErrorRepository errorRepository;
    private IngestionJobRepository jobRepository;
    private JdbcTemplate jdbcTemplate;
    private DynamicJdbcBatchWriter dynamicWriter;
    private BatchPersistenceService persistence;

    @BeforeEach
    void setUp() {
        recordRepository = mock(CustomerRecordRepository.class);
        errorRepository = mock(IngestionErrorRepository.class);
        jobRepository = mock(IngestionJobRepository.class);
        jdbcTemplate = mock(JdbcTemplate.class);
        dynamicWriter = mock(DynamicJdbcBatchWriter.class);

        persistence = new BatchPersistenceService(
                recordRepository,
                errorRepository,
                jobRepository,
                jdbcTemplate,
                dynamicWriter
        );
    }

    @Test
    void retrySkipsRowsAlreadyClaimedByTheLedger() {
        UUID jobId = UUID.randomUUID();
        IngestionJob job = mock(IngestionJob.class);

        DestinationTableMetadata metadata = new DestinationTableMetadata(
                "public", "customer_records", List.of()
        );

        DynamicCsvRow previouslyInserted = new DynamicCsvRow(
                2, Map.of("customer_id", "C001")
        );

        DynamicCsvRow notYetInserted = new DynamicCsvRow(
                3, Map.of("customer_id", "C002")
        );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        // First row is already claimed; second row is newly claimed.
        when(jdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(0, 1);

        // No destination uniqueness conflicts in this test.
        when(dynamicWriter.insertBatchWithConflictHandling(
                eq("public"),
                eq("customer_records"),
                eq(metadata),
                eq(List.of(notYetInserted))
        )).thenReturn(List.of());

        persistence.saveDynamicBatch(
                jobId,
                "public",
                "customer_records",
                metadata,
                List.of(previouslyInserted, notYetInserted),
                List.of(),
                2,
                2,
                0
        );

        verify(dynamicWriter).insertBatchWithConflictHandling(
                eq("public"),
                eq("customer_records"),
                eq(metadata),
                eq(List.of(notYetInserted))
        );

        verify(job).updateProgress(2, 2, 0);
        verify(errorRepository, never()).saveAll(any());
    }

    @Test
    void retryDoesNotPersistDuplicateValidationErrors() {
        UUID jobId = UUID.randomUUID();
        IngestionJob job = mock(IngestionJob.class);

        DestinationTableMetadata metadata = new DestinationTableMetadata(
                "public", "customer_records", List.of()
        );

        DynamicCsvValidationError existingError =
                new DynamicCsvValidationError(
                        2,
                        "email",
                        "INVALID_EMAIL",
                        "Invalid email"
                );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        // The rejected row is already recorded in the ledger.
        when(jdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(0);

        persistence.saveDynamicBatch(
                jobId,
                "public",
                "customer_records",
                metadata,
                List.of(),
                List.of(existingError),
                1,
                1,
                1
        );

        verify(errorRepository, never()).saveAll(any());

        // No valid rows were newly claimed, so the writer must not run.
        verify(dynamicWriter, never()).insertBatchWithConflictHandling(
                anyString(),
                anyString(),
                any(),
                any()
        );

        verify(job).updateProgress(1, 1, 1);
    }

    @Test
    void dynamicBatchPersistenceMustRunInsideATransaction()
            throws Exception {

        Method method = BatchPersistenceService.class.getMethod(
                "saveDynamicBatch",
                UUID.class,
                String.class,
                String.class,
                DestinationTableMetadata.class,
                List.class,
                List.class,
                long.class,
                long.class,
                long.class
        );

        Transactional transactional =
                method.getAnnotation(Transactional.class);

        assertNotNull(transactional);

        assertTrue(
                transactional.rollbackFor().length == 0,
                "Default Spring rollback behaviour should remain enabled"
        );
    }
}
