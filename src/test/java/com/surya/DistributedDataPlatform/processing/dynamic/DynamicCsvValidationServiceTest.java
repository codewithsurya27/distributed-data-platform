package com.surya.DistributedDataPlatform.processing.dynamic;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationColumnMetadata;
import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DynamicCsvValidationServiceTest {

    @TempDir
    Path tempDir;

    private final DynamicCsvValidationService service =
            new DynamicCsvValidationService();

    private DestinationTableMetadata metadata() {
        return new DestinationTableMetadata(
                "public",
                "customer_records",
                List.of(
                        new DestinationColumnMetadata(
                                "customer_id", "character varying",
                                "varchar", false, null,
                                100, null, null, true, 1
                        ),
                        new DestinationColumnMetadata(
                                "name", "character varying",
                                "varchar", false, null,
                                255, null, null, false, 2
                        ),
                        new DestinationColumnMetadata(
                                "email", "character varying",
                                "varchar", false, null,
                                320, null, null, false, 3
                        )
                )
        );
    }

    @Test
    void mapsCamelCaseCsvHeadersToSnakeCaseDatabaseColumns()
            throws Exception {

        Path csv = tempDir.resolve("customers.csv");

        Files.writeString(csv, """
                customerId,name,email
                C001,Alice,alice@example.com
                C002,Bob,bob@example.com
                """);

        DynamicCsvValidationResult result =
                service.validate(csv, metadata());

        assertEquals(2, result.totalRows());
        assertEquals(2, result.getValidRowCount());
        assertEquals(0, result.getFailedRowCount());

        assertEquals(
                "C001",
                result.validRows().get(0).values().get("customer_id")
        );
    }

    @Test
    void rejectsMissingRequiredDatabaseColumn() throws Exception {
        Path csv = tempDir.resolve("missing-column.csv");

        Files.writeString(csv, """
                customerId,name
                C001,Alice
                """);

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.validate(csv, metadata())
                );

        assertTrue(exception.getMessage().contains("email"));
    }

    @Test
    void rejectsUnknownCsvHeader() throws Exception {
        Path csv = tempDir.resolve("unknown-column.csv");

        Files.writeString(csv, """
                customerId,name,email,unknownField
                C001,Alice,alice@example.com,value
                """);

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.validate(csv, metadata())
                );

        assertTrue(exception.getMessage().contains("unknownField"));
    }

    @Test
    void returnsRowLevelErrorsForMissingRequiredValues()
            throws Exception {

        Path csv = tempDir.resolve("invalid-row.csv");

        Files.writeString(csv, """
                customerId,name,email
                C001,Alice,alice@example.com
                ,Bob,bob@example.com
                """);

        DynamicCsvValidationResult result =
                service.validate(csv, metadata());

        assertEquals(2, result.totalRows());
        assertEquals(1, result.getValidRowCount());
        assertEquals(1, result.getFailedRowCount());
        assertEquals(
                "REQUIRED_VALUE_MISSING",
                result.errors().get(0).errorCode()
        );
    }
}