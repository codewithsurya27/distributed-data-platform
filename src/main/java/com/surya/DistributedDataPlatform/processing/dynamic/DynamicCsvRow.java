package com.surya.DistributedDataPlatform.processing.dynamic;

import java.util.Map;

public record DynamicCsvRow(
        long rowNumber,
        Map<String, String> values
) {
}