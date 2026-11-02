
package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

@Service
public class DynamicJdbcBatchWriter {

    private static final Pattern IDENTIFIER_PATTERN =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_$]{0,62}");

    private final JdbcTemplate jdbcTemplate;

    public DynamicJdbcBatchWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Inserts a batch of validated CSV rows into the requested PostgreSQL table.
     *
     * Only columns present in the CSV rows are included in the INSERT.
     * Columns omitted from the CSV are left to PostgreSQL defaults/nullability.
     *
     * @return number of submitted rows
     */
    public int insertBatch(
            String schema,
            String table,
            DestinationTableMetadata metadata,
            List<DynamicCsvRow> rows
    ) {
        Objects.requireNonNull(metadata, "Destination metadata is required");
        Objects.requireNonNull(rows, "Rows are required");

        if (rows.isEmpty()) {
            return 0;
        }

        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");

        if (!schema.equals(metadata.schemaName())
                || !table.equals(metadata.tableName())) {
            throw new IllegalArgumentException(
                    "Destination does not match the supplied table metadata"
            );
        }

        Map<String, DestinationColumnMetadata> columnsByName =
                new LinkedHashMap<>();

        for (DestinationColumnMetadata column : metadata.columns()) {
            columnsByName.put(column.columnName(), column);
        }

        List<String> insertColumns =
                new ArrayList<>(rows.get(0).values().keySet());

        if (insertColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "CSV rows do not contain any destination columns"
            );
        }

        for (String columnName : insertColumns) {
            if (!columnsByName.containsKey(columnName)) {
                throw new IllegalArgumentException(
                        "Unknown destination column: " + columnName
                );
            }
        }

        Set<String> expectedColumns = Set.copyOf(insertColumns);

        for (DynamicCsvRow row : rows) {
            if (!row.values().keySet().equals(expectedColumns)) {
                throw new IllegalArgumentException(
                        "All rows in a batch must contain the same columns"
                );
            }
        }

        String sql = buildInsertSql(
                schema,
                table,
                insertColumns,
                columnsByName
        );

        List<Object[]> batchArguments = new ArrayList<>(rows.size());

        for (DynamicCsvRow row : rows) {
            Object[] arguments = new Object[insertColumns.size()];

            for (int i = 0; i < insertColumns.size(); i++) {
                arguments[i] = row.values().get(insertColumns.get(i));
            }

            batchArguments.add(arguments);
        }

        jdbcTemplate.batchUpdate(sql, batchArguments);

        return rows.size();
    }

    /**
     * Inserts rows individually, ignoring PostgreSQL unique/exclusion
     * conflicts. Returns rows that were rejected because of a conflict.
     *
     * ON CONFLICT DO NOTHING handles all applicable unique constraints,
     * including primary keys and unique indexes.
     *
     * Other database errors are not suppressed.
     */
    public List<DynamicCsvRow> insertBatchWithConflictHandling(
            String schema,
            String table,
            DestinationTableMetadata metadata,
            List<DynamicCsvRow> rows
    ) {
        Objects.requireNonNull(metadata, "Destination metadata is required");
        Objects.requireNonNull(rows, "Rows are required");

        if (rows.isEmpty()) {
            return List.of();
        }

        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");

        if (!schema.equals(metadata.schemaName())
                || !table.equals(metadata.tableName())) {
            throw new IllegalArgumentException(
                    "Destination does not match the supplied table metadata"
            );
        }

        Map<String, DestinationColumnMetadata> columnsByName =
                new LinkedHashMap<>();

        for (DestinationColumnMetadata column : metadata.columns()) {
            columnsByName.put(column.columnName(), column);
        }

        List<String> insertColumns =
                new ArrayList<>(rows.get(0).values().keySet());

        if (insertColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "CSV rows do not contain any destination columns"
            );
        }

        for (String columnName : insertColumns) {
            if (!columnsByName.containsKey(columnName)) {
                throw new IllegalArgumentException(
                        "Unknown destination column: " + columnName
                );
            }
        }

        Set<String> expectedColumns = Set.copyOf(insertColumns);

        for (DynamicCsvRow row : rows) {
            if (!row.values().keySet().equals(expectedColumns)) {
                throw new IllegalArgumentException(
                        "All rows in a batch must contain the same columns"
                );
            }
        }

        String sql = buildInsertSql(
                schema,
                table,
                insertColumns,
                columnsByName
        ) + " ON CONFLICT DO NOTHING";

        List<DynamicCsvRow> rejectedRows = new ArrayList<>();

        for (DynamicCsvRow row : rows) {
            Object[] arguments = new Object[insertColumns.size()];

            for (int i = 0; i < insertColumns.size(); i++) {
                arguments[i] = row.values().get(insertColumns.get(i));
            }

            int inserted = jdbcTemplate.update(sql, arguments);

            if (inserted == 0) {
                rejectedRows.add(row);
            }
        }

        return rejectedRows;
    }

    private String buildInsertSql(
            String schema,
            String table,
            List<String> columns,
            Map<String, DestinationColumnMetadata> metadata
    ) {
        String columnSql = columns.stream()
                .map(this::quoteIdentifier)
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();

        String placeholders = columns.stream()
                .map(columnName -> {
                    DestinationColumnMetadata column =
                            metadata.get(columnName);

                    return "CAST(? AS " + postgresCastType(column) + ")";
                })
                .reduce((left, right) -> left + ", " + right)
                .orElseThrow();

        return "INSERT INTO "
                + quoteIdentifier(schema)
                + "."
                + quoteIdentifier(table)
                + " ("
                + columnSql
                + ") VALUES ("
                + placeholders
                + ")";
    }

    /**
     * Returns only explicitly supported PostgreSQL cast types.
     * Type names come from database metadata, never directly from CSV input.
     */
    private String postgresCastType(DestinationColumnMetadata column) {

        String dataType = column.dataType()
                .toLowerCase()
                .trim();

        return switch (dataType) {
            case "character varying", "varchar" -> "varchar";
            case "character", "char" -> "char";
            case "text" -> "text";

            case "smallint", "int2" -> "smallint";
            case "integer", "int", "int4" -> "integer";
            case "bigint", "int8" -> "bigint";

            case "numeric", "decimal" -> "numeric";
            case "real", "float4" -> "real";
            case "double precision", "float8" -> "double precision";

            case "boolean", "bool" -> "boolean";

            case "date" -> "date";
            case "time without time zone" -> "time";
            case "time with time zone" -> "timetz";
            case "timestamp without time zone" -> "timestamp";
            case "timestamp with time zone" -> "timestamptz";

            case "uuid" -> "uuid";
            case "json" -> "json";
            case "jsonb" -> "jsonb";
            case "bytea" -> "bytea";

            default -> throw new IllegalArgumentException(
                    "Unsupported PostgreSQL data type for column "
                            + column.columnName()
                            + ": "
                            + column.dataType()
            );
        };
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()
                || identifier.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "Invalid PostgreSQL identifier"
            );
        }

        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void validateIdentifier(String identifier, String type) {
        if (identifier == null
                || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    "Invalid PostgreSQL " + type + " name"
            );
        }
    }
}
