package com.surya.DistributedDataPlatform.jobs.dto;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJobStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

public record IngestionJobResponse(
        UUID id,
        String fileName,
        String filePath,
        IngestionJobStatus status,
        long totalRows,
        long processedRows,
        long failedRows,
        OffsetDateTime createdAt,
        OffsetDateTime startedAt,
        OffsetDateTime completedAt
) {
    public static IngestionJobResponse from(IngestionJob job) {
        return new IngestionJobResponse(
                job.getId(),
                job.getFileName(),
                job.getFilePath(),
                job.getStatus(),
                job.getTotalRows(),
                job.getProcessedRows(),
                job.getFailedRows(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt()
        );
    }
}
