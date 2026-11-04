
package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamicJdbcBatchWriterTest {

    private JdbcTemplate jdbcTemplate;
    private DynamicJdbcBatchWriter writer;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        writer = new DynamicJdbcBatchWriter(jdbcTemplate);
    }

    @Test
    void shouldBuildDynamicInsertAndSubmitBatch() {

        DestinationTableMetadata metadata = customerMetadata();

        List<DynamicCsvRow> rows = List.of(
                new DynamicCsvRow(
                        2,
                        Map.of(
                                "customer_id", "C001",
                                "name", "Alice",
                                "email", "alice@example.com"
                        )
                ),
                new DynamicCsvRow(
                        3,
                        Map.of(
                                "customer_id", "C002",
                                "name", "Bob",
                                "email", "bob@example.com"
                        )
                )
        );

        when(jdbcTemplate.batchUpdate(anyString(), anyList()))
                .thenReturn(new int[]{1, 1});

        int inserted = writer.insertBatch(
                "public",
                "customer_records",
                metadata,
                rows
        );

        assertEquals(2, inserted);

        verify(jdbcTemplate).batchUpdate(
                anyString(),
                anyList()
        );
    }

    @Test
    void shouldRejectUnknownDestinationColumn() {

        DestinationTableMetadata metadata = customerMetadata();

        List<DynamicCsvRow> rows = List.of(
                new DynamicCsvRow(
                        2,
                        Map.of("unknown_column", "value")
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> writer.insertBatch(
                        "public",
                        "customer_records",
                        metadata,
                        rows
                )
        );
    }

    @Test
    void shouldRejectMetadataForDifferentDestination() {

        assertThrows(
                IllegalArgumentException.class,
                () -> writer.insertBatch(
                        "public",
                        "another_table",
                        customerMetadata(),
                        List.of(
                                new DynamicCsvRow(
                                        2,
                                        Map.of("customer_id", "C001")
                                )
                        )
                )
        );
    }

    @Test
    void shouldReturnZeroForEmptyBatch() {

        int inserted = writer.insertBatch(
                "public",
                "customer_records",
                customerMetadata(),
                List.of()
        );

        assertEquals(0, inserted);
    }

    private DestinationTableMetadata customerMetadata() {

        return new DestinationTableMetadata(
                "public",
                "customer_records",
                List.of(
                        new DestinationColumnMetadata(
                                "customer_id",
                                "character varying",
                                "varchar",
                                false,
                                null,
                                100,
                                null,
                                null,
                                true,
                                1
                        ),
                        new DestinationColumnMetadata(
                                "name",
                                "character varying",
                                "varchar",
                                false,
                                null,
                                255,
                                null,
                                null,
                                false,
                                2
                        ),
                        new DestinationColumnMetadata(
                                "email",
                                "character varying",
                                "varchar",
                                false,
                                null,
                                255,
                                null,
                                null,
                                false,
                                3
                        )
                )
        );
    }
}
