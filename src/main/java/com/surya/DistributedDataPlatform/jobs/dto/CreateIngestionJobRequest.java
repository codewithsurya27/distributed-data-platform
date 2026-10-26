package com.surya.DistributedDataPlatform.jobs.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateIngestionJobRequest(
        @NotBlank @Size(max = 255) String fileName,
        @Size(max = 1000) String filePath
) {
}
