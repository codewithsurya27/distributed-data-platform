package com.surya.DistributedDataPlatform.jobs.service;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
public class PostgreSqlDestinationService {

    private static final String IDENTIFIER_PATTERN =
            "[A-Za-z_][A-Za-z0-9_$]{0,62}";

    private final JdbcTemplate jdbcTemplate;

    public PostgreSqlDestinationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void validateDestination(String schema, String table) {

        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");

        if (schema.equalsIgnoreCase("information_schema")
                || schema.equalsIgnoreCase("pg_catalog")
                || schema.toLowerCase().startsWith("pg_toast")) {

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "System schemas cannot be used as ingestion destinations"
            );
        }

        Integer count = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ?
                  AND table_name = ?
                  AND table_type = 'BASE TABLE'
                """,
                Integer.class,
                schema,
                table
        );

        if (count == null || count == 0) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Destination table does not exist: " + schema + "." + table
            );
        }
    }

    public DestinationTableMetadata getTableMetadata(
            String schema,
            String table
    ) {

        validateDestination(schema, table);

        List<DestinationColumnMetadata> columns =
                jdbcTemplate.query(
                        """
                        SELECT
                            c.column_name,
                            c.data_type,
                            c.udt_name,
                            c.is_nullable,
                            c.column_default,
                            c.character_maximum_length,
                            c.numeric_precision,
                            c.numeric_scale,
                            c.ordinal_position,
                            EXISTS (
                                SELECT 1
                                FROM information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kcu
                                  ON tc.constraint_catalog = kcu.constraint_catalog
                                 AND tc.constraint_schema = kcu.constraint_schema
                                 AND tc.constraint_name = kcu.constraint_name
                                 AND tc.table_schema = kcu.table_schema
                                 AND tc.table_name = kcu.table_name
                                WHERE tc.constraint_type = 'PRIMARY KEY'
                                  AND tc.table_schema = c.table_schema
                                  AND tc.table_name = c.table_name
                                  AND kcu.column_name = c.column_name
                            ) AS is_primary_key
                        FROM information_schema.columns c
                        WHERE c.table_schema = ?
                          AND c.table_name = ?
                        ORDER BY c.ordinal_position
                        """,
                        (rs, rowNum) -> new DestinationColumnMetadata(
                                rs.getString("column_name"),
                                rs.getString("data_type"),
                                rs.getString("udt_name"),
                                "YES".equals(rs.getString("is_nullable")),
                                rs.getString("column_default"),
                                toInteger(rs.getObject("character_maximum_length")),
                                toInteger(rs.getObject("numeric_precision")),
                                toInteger(rs.getObject("numeric_scale")),
                                rs.getBoolean("is_primary_key"),
                                rs.getInt("ordinal_position")
                        ),
                        schema,
                        table
                );

        return new DestinationTableMetadata(
                schema,
                table,
                columns
        );
    }

    private Integer toInteger(Object value) {
        return value == null ? null : ((Number) value).intValue();
    }

    private void validateIdentifier(String identifier, String type) {

        if (identifier == null
                || !identifier.matches(IDENTIFIER_PATTERN)) {

            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid PostgreSQL " + type + " name"
            );
        }
    }
}