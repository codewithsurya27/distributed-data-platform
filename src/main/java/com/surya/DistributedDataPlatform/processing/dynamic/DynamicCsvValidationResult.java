package com.surya.DistributedDataPlatform.processing.dynamic;

import java.util.List;

public record DynamicCsvValidationResult(
        long totalRows,
        List<DynamicCsvRow> validRows,
        List<DynamicCsvValidationError> errors
) {
    public long getValidRowCount() {
        return validRows.size();
    }

    public long getFailedRowCount() {
        return errors.stream()
                .map(DynamicCsvValidationError::rowNumber)
                .distinct()
                .count();
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}