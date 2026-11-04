
package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers(disabledWithoutDocker = true)
class DynamicPostgreSqlIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17-alpine");

    private JdbcTemplate jdbcTemplate;
    private DynamicJdbcBatchWriter writer;
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        var dataSource = POSTGRES.getJdbcUrl();

        org.springframework.jdbc.datasource.DriverManagerDataSource actualDataSource =
                new org.springframework.jdbc.datasource.DriverManagerDataSource(
                        dataSource,
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword()
                );

        jdbcTemplate = new JdbcTemplate(actualDataSource);
        writer = new DynamicJdbcBatchWriter(jdbcTemplate);
        transactionTemplate = new TransactionTemplate(
                new DataSourceTransactionManager(actualDataSource)
        );

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS public.dynamic_ingestion_rows (
                    job_id UUID NOT NULL,
                    row_number BIGINT NOT NULL,
                    target_schema VARCHAR(63) NOT NULL,
                    target_table VARCHAR(63) NOT NULL,
                    outcome VARCHAR(20) NOT NULL,
                    PRIMARY KEY (job_id, row_number)
                )
                """);

        jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS integration_a");
        jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS integration_b");

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS integration_a.people (
                    id BIGSERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    age INTEGER NOT NULL
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS integration_b.orders (
                    id BIGSERIAL PRIMARY KEY,
                    reference VARCHAR(100) NOT NULL,
                    quantity INTEGER NOT NULL
                )
                """);

        jdbcTemplate.update("DELETE FROM public.dynamic_ingestion_rows");
        jdbcTemplate.update("DELETE FROM integration_a.people");
        jdbcTemplate.update("DELETE FROM integration_b.orders");
    }

    @Test
    void rollsBackLedgerClaimWhenDestinationInsertFails() {
        UUID jobId = UUID.randomUUID();

        DynamicCsvRow invalidRow = new DynamicCsvRow(
                1L,
                values("name", "Alice", "age", "not-a-number")
        );

        assertThrows(RuntimeException.class, () ->
                transactionTemplate.executeWithoutResult(status -> {
                    claimRow(jobId, 1L, "integration_a", "people");

                    writer.insertBatch(
                            "integration_a",
                            "people",
                            peopleMetadata(),
                            List.of(invalidRow)
                    );
                })
        );

        assertEquals(
                0,
                count(
                        "SELECT COUNT(*) FROM public.dynamic_ingestion_rows WHERE job_id = ?",
                        jobId
                )
        );

        assertEquals(
                0,
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM integration_a.people",
                        Integer.class
                )
        );
    }

    @Test
    void retryDoesNotInsertTheSameRowTwice() {
        UUID jobId = UUID.randomUUID();

        DynamicCsvRow row = new DynamicCsvRow(
                1L,
                values("name", "Alice", "age", "30")
        );

        // First attempt claims the row and inserts it.
        transactionTemplate.executeWithoutResult(status -> {
            if (claimRow(jobId, 1L, "integration_a", "people")) {
                writer.insertBatch(
                        "integration_a",
                        "people",
                        peopleMetadata(),
                        List.of(row)
                );
            }
        });

        // Retry sees the existing ledger entry and skips the insert.
        transactionTemplate.executeWithoutResult(status -> {
            if (claimRow(jobId, 1L, "integration_a", "people")) {
                writer.insertBatch(
                        "integration_a",
                        "people",
                        peopleMetadata(),
                        List.of(row)
                );
            }
        });

        assertEquals(
                1,
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM integration_a.people",
                        Integer.class
                )
        );

        assertEquals(
                1,
                count(
                        "SELECT COUNT(*) FROM public.dynamic_ingestion_rows WHERE job_id = ?",
                        jobId
                )
        );
    }

    @Test
    void writesToDifferentSchemasAndTablesUsingTheirMetadata() {
        DynamicCsvRow person = new DynamicCsvRow(
                1L,
                values("name", "Bob", "age", "28")
        );

        DynamicCsvRow order = new DynamicCsvRow(
                1L,
                values("reference", "ORDER-1001", "quantity", "4")
        );

        writer.insertBatch(
                "integration_a",
                "people",
                peopleMetadata(),
                List.of(person)
        );

        writer.insertBatch(
                "integration_b",
                "orders",
                ordersMetadata(),
                List.of(order)
        );

        assertEquals(
                1,
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM integration_a.people WHERE name = 'Bob'",
                        Integer.class
                )
        );

        assertEquals(
                1,
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM integration_b.orders WHERE reference = 'ORDER-1001'",
                        Integer.class
                )
        );
    }

    @Test
    void rejectsUnsafeDestinationIdentifiersBeforeExecutingSql() {
        DynamicCsvRow row = new DynamicCsvRow(
                1L,
                values("name", "Alice", "age", "30")
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> writer.insertBatch(
                        "integration_a; DROP SCHEMA public",
                        "people",
                        peopleMetadata(),
                        List.of(row)
                )
        );

        assertEquals(
                0,
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM integration_a.people",
                        Integer.class
                )
        );
    }

    private boolean claimRow(
            UUID jobId,
            long rowNumber,
            String schema,
            String table
    ) {
        return jdbcTemplate.update("""
                INSERT INTO public.dynamic_ingestion_rows
                    (job_id, row_number, target_schema, target_table, outcome)
                VALUES (?, ?, ?, ?, 'INSERTED')
                ON CONFLICT (job_id, row_number) DO NOTHING
                """,
                jobId,
                rowNumber,
                schema,
                table
        ) == 1;
    }

    private int count(String sql, UUID jobId) {
        return jdbcTemplate.queryForObject(sql, Integer.class, jobId);
    }

    private DestinationTableMetadata peopleMetadata() {
        return new DestinationTableMetadata(
                "integration_a",
                "people",
                List.of(
                        column("name", "character varying", true, 1),
                        column("age", "integer", true, 2)
                )
        );
    }

    private DestinationTableMetadata ordersMetadata() {
        return new DestinationTableMetadata(
                "integration_b",
                "orders",
                List.of(
                        column("reference", "character varying", true, 1),
                        column("quantity", "integer", true, 2)
                )
        );
    }

    private DestinationColumnMetadata column(
            String name,
            String type,
            boolean nullable,
            int ordinal
    ) {
        return new DestinationColumnMetadata(
                name,
                type,
                type,
                nullable,
                null,
                null,
                null,
                null,
                false,
                ordinal
        );
    }

    private Map<String, String> values(String... keyValues) {
        Map<String, String> values = new LinkedHashMap<>();

        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }

        return values;
    }
}
