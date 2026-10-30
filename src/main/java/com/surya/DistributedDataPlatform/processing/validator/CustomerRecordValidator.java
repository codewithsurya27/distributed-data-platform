package com.surya.DistributedDataPlatform.processing.validator;

import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.regex.Pattern;

@Component
public class CustomerRecordValidator {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");

    public String validate(CSVRecord record) {
        String customerId = value(record, "customerId");
        String name = value(record, "name");
        String email = value(record, "email");

        if (customerId.isBlank()) {
            return "customerId is required";
        }

        if (customerId.length() > 100) {
            return "customerId must not exceed 100 characters";
        }

        if (name.isBlank()) {
            return "name is required";
        }

        if (name.length() > 255) {
            return "name must not exceed 255 characters";
        }

        if (email.isBlank() ||
                email.length() > 320 ||
                !EMAIL_PATTERN.matcher(email).matches()) {
            return "email is invalid";
        }

        return null;
    }

    public String value(CSVRecord record, String column) {
        for (String header : record.getParser().getHeaderMap().keySet()) {
            if (header.trim().toLowerCase(Locale.ROOT)
                    .equals(column.toLowerCase(Locale.ROOT))) {
                return record.get(header) == null
                        ? "" : record.get(header).trim();
            }
        }

        return "";
    }
}