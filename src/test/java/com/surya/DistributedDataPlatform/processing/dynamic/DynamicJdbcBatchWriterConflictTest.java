package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamicJdbcBatchWriterConflictTest {

    private JdbcTemplate jdbcTemplate;
    private DynamicJdbcBatchWriter writer;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        writer = new DynamicJdbcBatchWriter(jdbcTemplate);
    }

    @Test
    void shouldReturnOnlyRowsRejectedByUniqueConstraints() {

        DestinationTableMetadata metadata =
                new DestinationTableMetadata(
                        "public",
                        "phase9_people",
                        List.of(
                                new DestinationColumnMetadata(
                                        "person_id",
                                        "bigint",
                                        "int8",
                                        false,
                                        null,
                                        null,
                                        null,
                                        null,
                                        true,
                                        1
                                ),
                                new DestinationColumnMetadata(
                                        "full_name",
                                        "character varying",
                                        "varchar",
                                        false,
                                        null,
                                        100,
                                        null,
                                        null,
                                        false,
                                        2
                                )
                        )
                );

        DynamicCsvRow first = new DynamicCsvRow(
                2,
                Map.of("person_id", "1001", "full_name", "Alice")
        );

        DynamicCsvRow second = new DynamicCsvRow(
                3,
                Map.of("person_id", "1002", "full_name", "Bob")
        );

        DynamicCsvRow third = new DynamicCsvRow(
                4,
                Map.of("person_id", "1003", "full_name", "Charlie")
        );

        when(jdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(1, 0, 1);

        List<DynamicCsvRow> rejected =
                writer.insertBatchWithConflictHandling(
                        "public",
                        "phase9_people",
                        metadata,
                        List.of(first, second, third)
                );

        assertEquals(1, rejected.size());
        assertEquals(3, rejected.get(0).rowNumber());

        verify(jdbcTemplate, org.mockito.Mockito.times(3))
                .update(anyString(), any(Object[].class));
    }
}