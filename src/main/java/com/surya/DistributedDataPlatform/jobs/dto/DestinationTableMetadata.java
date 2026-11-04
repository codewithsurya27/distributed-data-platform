package com.surya.DistributedDataPlatform.jobs.dto;

import java.util.List;

public record DestinationTableMetadata(
        String schemaName,
        String tableName,
        List<DestinationColumnMetadata> columns
) {
}