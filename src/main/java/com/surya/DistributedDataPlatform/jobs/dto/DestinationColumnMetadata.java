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
        int ordinalPosition,
        boolean identity,
        boolean generated
) {

    /**
     * Backwards-compatible constructor for callers/tests created before
     * PostgreSQL identity/generated-column metadata was exposed.
     */
    public DestinationColumnMetadata(
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
        this(
                columnName,
                dataType,
                databaseType,
                nullable,
                defaultValue,
                characterMaximumLength,
                numericPrecision,
                numericScale,
                primaryKey,
                ordinalPosition,
                false,
                false
        );
    }
}
