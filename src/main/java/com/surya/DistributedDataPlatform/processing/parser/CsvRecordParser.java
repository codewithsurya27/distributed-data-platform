package com.surya.DistributedDataPlatform.processing.parser;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;

@Component
public class CsvRecordParser {

    private static final Set<String> REQUIRED_HEADERS =
            Set.of("customerid", "name", "email");

    public CSVParser parse(Path path) throws IOException {
        Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);

        try {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .get();

            CSVParser parser = format.parse(reader);

            Set<String> headers = parser.getHeaderMap().keySet().stream()
                    .map(h -> h.trim().toLowerCase(Locale.ROOT))
                    .collect(java.util.stream.Collectors.toSet());

            if (!headers.containsAll(REQUIRED_HEADERS)) {
                parser.close();
                throw new IOException(
                        "CSV must contain customerId, name, and email headers");
            }

            return parser;
        } catch (Exception ex) {
            reader.close();
            throw ex;
        }
    }
}