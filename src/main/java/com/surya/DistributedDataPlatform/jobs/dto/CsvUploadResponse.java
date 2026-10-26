package com.surya.DistributedDataPlatform.jobs.dto;

import java.util.UUID;

public record CsvUploadResponse(
        UUID jobId,
        String fileName,
        String status,
        String message
) {
}
