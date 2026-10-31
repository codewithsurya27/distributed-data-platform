
package com.surya.DistributedDataPlatform.processing.validator;

import com.surya.DistributedDataPlatform.processing.parser.CsvRecordParser;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;

class CustomerRecordValidatorTest {

    private final CustomerRecordValidator validator =
            new CustomerRecordValidator();

    private CSVRecord createRecord(String csv) throws Exception {
        CSVParser parser = new CsvRecordParser().parse(
                createTempCsv(csv)
        );
        return parser.getRecords().get(0);
    }

    private java.nio.file.Path createTempCsv(String csv) throws Exception {
        java.nio.file.Path path =
                java.nio.file.Files.createTempFile("customer-test", ".csv");
        java.nio.file.Files.writeString(path, csv);
        return path;
    }

    @Test
    void acceptsValidCustomerRecord() throws Exception {
        CSVRecord record = createRecord(
                "customerId,name,email\nC001,Alice,alice@example.com\n"
        );

        assertNull(validator.validate(record));
    }

    @Test
    void rejectsBlankCustomerId() throws Exception {
        CSVRecord record = createRecord(
                "customerId,name,email\n,Alice,alice@example.com\n"
        );

        assertEquals(
                "customerId is required",
                validator.validate(record)
        );
    }

    @Test
    void rejectsInvalidEmail() throws Exception {
        CSVRecord record = createRecord(
                "customerId,name,email\nC001,Alice,invalid-email\n"
        );

        assertEquals(
                "email is invalid",
                validator.validate(record)
        );
    }

    @Test
    void rejectsBlankName() throws Exception {
        CSVRecord record = createRecord(
                "customerId,name,email\nC001,,alice@example.com\n"
        );

        assertEquals(
                "name is required",
                validator.validate(record)
        );
    }
}
