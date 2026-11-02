package com.surya.DistributedDataPlatform.jobs.dto;

public record DestinationColumnMetadata(
        String columnName,
        String dataType,
        String databaseType,
        boolean nullable,
        String defaultValue,
        Integer characterMaximumLength,
        Integer numericPrecision,
        Integer numericScale,
        boolean primaryKey,
        int ordinalPosition
) {
}