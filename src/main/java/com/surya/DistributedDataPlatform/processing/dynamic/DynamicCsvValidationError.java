package com.surya.DistributedDataPlatform.processing.dynamic;

public record DynamicCsvValidationError(
        long rowNumber,
        String columnName,
        String errorCode,
        String message
) {
}