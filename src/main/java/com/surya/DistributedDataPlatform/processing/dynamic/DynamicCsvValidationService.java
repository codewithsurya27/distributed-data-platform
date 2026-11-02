package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
public class DynamicCsvValidationService {

    private static final Pattern INTEGER_PATTERN =
            Pattern.compile("[+-]?\\d+");

    public DynamicCsvValidationResult validate(
            Path csvPath,
            DestinationTableMetadata metadata
    ) throws IOException {

        if (metadata == null || metadata.columns() == null
                || metadata.columns().isEmpty()) {
            throw new IllegalArgumentException(
                    "Destination table metadata is empty"
            );
        }

        try (Reader reader = Files.newBufferedReader(
                csvPath, StandardCharsets.UTF_8
        );
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setTrim(true)
                     .get()
                     .parse(reader)) {

            Map<String, DestinationColumnMetadata> databaseColumns =
                    buildDatabaseColumnMap(metadata);

            Map<String, String> headerMapping =
                    mapHeaders(parser.getHeaderMap().keySet(), databaseColumns);

            validateRequiredColumns(databaseColumns, headerMapping);

            List<DynamicCsvRow> validRows = new ArrayList<>();
            List<DynamicCsvValidationError> errors = new ArrayList<>();

            long totalRows = 0;

            for (CSVRecord record : parser) {
                totalRows++;

                Map<String, String> mappedValues = new LinkedHashMap<>();
                List<DynamicCsvValidationError> rowErrors =
                        new ArrayList<>();

                for (Map.Entry<String, String> mapping
                        : headerMapping.entrySet()) {

                    String csvHeader = mapping.getKey();
                    String databaseColumn = mapping.getValue();

                    DestinationColumnMetadata column =
                            databaseColumns.get(normalize(databaseColumn));

                    String value = record.get(csvHeader);

                    if (value != null) {
                        value = value.trim();
                    }

                    if (value == null || value.isEmpty()) {
                        if (!column.nullable()
                                && column.defaultValue() == null) {

                            rowErrors.add(new DynamicCsvValidationError(
                                    record.getRecordNumber() + 1,
                                    databaseColumn,
                                    "REQUIRED_VALUE_MISSING",
                                    databaseColumn + " cannot be empty"
                            ));
                        }

                        mappedValues.put(databaseColumn, null);
                        continue;
                    }

                    String typeError = validateValue(value, column);

                    if (typeError != null) {
                        rowErrors.add(new DynamicCsvValidationError(
                                record.getRecordNumber() + 1,
                                databaseColumn,
                                "INVALID_DATA_TYPE",
                                typeError
                        ));
                    } else {
                        mappedValues.put(databaseColumn, value);
                    }
                }

                if (rowErrors.isEmpty()) {
                    validRows.add(new DynamicCsvRow(
                            record.getRecordNumber() + 1,
                            java.util.Collections.unmodifiableMap(
                                    new LinkedHashMap<>(mappedValues)
                            )
                    ));
                } else {
                    errors.addAll(rowErrors);
                }
            }

            return new DynamicCsvValidationResult(
                    totalRows,
                    List.copyOf(validRows),
                    List.copyOf(errors)
            );
        }
    }

    private Map<String, DestinationColumnMetadata> buildDatabaseColumnMap(
            DestinationTableMetadata metadata
    ) {

        Map<String, DestinationColumnMetadata> columns =
                new LinkedHashMap<>();

        for (DestinationColumnMetadata column : metadata.columns()) {
            String normalized = normalize(column.columnName());

            if (columns.put(normalized, column) != null) {
                throw new IllegalArgumentException(
                        "Ambiguous destination column: "
                                + column.columnName()
                );
            }
        }

        return columns;
    }

    private Map<String, String> mapHeaders(
            Set<String> csvHeaders,
            Map<String, DestinationColumnMetadata> databaseColumns
    ) {

        if (csvHeaders == null || csvHeaders.isEmpty()) {
            throw new IllegalArgumentException(
                    "CSV must contain a header row"
            );
        }

        Map<String, String> mapping = new LinkedHashMap<>();
        Set<String> mappedColumns = new HashSet<>();

        for (String header : csvHeaders) {
            if (header == null || header.isBlank()) {
                throw new IllegalArgumentException(
                        "CSV contains an empty column header"
                );
            }

            String normalizedHeader = normalize(header);

            DestinationColumnMetadata column =
                    databaseColumns.get(normalizedHeader);

            if (column == null) {
                throw new IllegalArgumentException(
                        "CSV column does not exist in destination table: "
                                + header
                );
            }

            if (!mappedColumns.add(normalizedHeader)) {
                throw new IllegalArgumentException(
                        "Duplicate CSV column mapping: " + header
                );
            }

            mapping.put(header, column.columnName());
        }

        return mapping;
    }

    private void validateRequiredColumns(
            Map<String, DestinationColumnMetadata> databaseColumns,
            Map<String, String> headerMapping
    ) {

        Set<String> mappedColumns = new HashSet<>();

        headerMapping.values().forEach(
                column -> mappedColumns.add(normalize(column))
        );

        for (DestinationColumnMetadata column
                : databaseColumns.values()) {

            boolean required =
                    !column.nullable()
                            && column.defaultValue() == null;

            if (required
                    && !mappedColumns.contains(
                    normalize(column.columnName()))) {

                throw new IllegalArgumentException(
                        "Required destination column is missing from CSV: "
                                + column.columnName()
                );
            }
        }
    }

    private String validateValue(
            String value,
            DestinationColumnMetadata column
    ) {

        String type = column.dataType().toLowerCase(Locale.ROOT);
        String databaseType =
                column.databaseType().toLowerCase(Locale.ROOT);

        try {
            switch (type) {
                case "character varying", "character", "text" -> {
                    if (column.characterMaximumLength() != null
                            && value.length()
                            > column.characterMaximumLength()) {
                        return "Value exceeds maximum length of "
                                + column.characterMaximumLength();
                    }
                }

                case "smallint" -> {
                    if (!INTEGER_PATTERN.matcher(value).matches()) {
                        return "Expected a SMALLINT value";
                    }
                    Short.parseShort(value);
                }

                case "integer" -> {
                    if (!INTEGER_PATTERN.matcher(value).matches()) {
                        return "Expected an INTEGER value";
                    }
                    Integer.parseInt(value);
                }

                case "bigint" -> {
                    if (!INTEGER_PATTERN.matcher(value).matches()) {
                        return "Expected a BIGINT value";
                    }
                    Long.parseLong(value);
                }

                case "numeric", "decimal" -> {
                    BigDecimal number = new BigDecimal(value);

                    if (column.numericScale() != null
                            && Math.max(number.scale(), 0)
                            > column.numericScale()) {
                        return "Value exceeds numeric scale of "
                                + column.numericScale();
                    }

                    if (column.numericPrecision() != null
                            && number.precision()
                            > column.numericPrecision()) {
                        return "Value exceeds numeric precision of "
                                + column.numericPrecision();
                    }
                }

                case "real" -> Float.parseFloat(value);

                case "double precision" -> Double.parseDouble(value);

                case "boolean" -> {
                    if (!Set.of("true", "false", "t", "f", "1", "0")
                            .contains(value.toLowerCase(Locale.ROOT))) {
                        return "Expected a BOOLEAN value";
                    }
                }

                case "date" -> LocalDate.parse(value);

                case "time without time zone" -> LocalTime.parse(value);

                case "time with time zone" -> OffsetTime.parse(value);

                case "timestamp without time zone" ->
                        LocalDateTime.parse(value.replace(' ', 'T'));

                case "timestamp with time zone" ->
                        OffsetDateTime.parse(value.replace(' ', 'T'));

                case "uuid" -> UUID.fromString(value);

                case "json", "jsonb" -> {
                    if (!isValidJson(value)) {
                        return "Expected valid JSON";
                    }
                }

                case "bytea" -> {
                    if (!value.startsWith("\\x")) {
                        return "Expected hexadecimal bytea value beginning with \\x";
                    }
                    if ((value.length() - 2) % 2 != 0) {
                        return "Invalid hexadecimal bytea value";
                    }
                    if (!value.substring(2).matches("[0-9a-fA-F]*")) {
                        return "Invalid hexadecimal bytea value";
                    }
                }

                default -> {
                    // Unknown PostgreSQL types are not rejected here.
                    // PostgreSQL remains the final authority at insertion.
                }
            }

            // PostgreSQL internal types can differ from data_type.
            if ("_text".equals(databaseType) && !value.startsWith("{")) {
                return "Expected a PostgreSQL text array";
            }

            return null;

        } catch (NumberFormatException ex) {
            return "Invalid " + column.dataType() + " value";
        } catch (RuntimeException ex) {
            return "Invalid " + column.dataType() + " value";
        }
    }

    private boolean isValidJson(String value) {
        try {
            new com.fasterxml.jackson.databind.ObjectMapper()
                    .readTree(value);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private String normalize(String value) {
        return value.replaceAll("[_\\s-]", "")
                .toLowerCase(Locale.ROOT);
    }
}