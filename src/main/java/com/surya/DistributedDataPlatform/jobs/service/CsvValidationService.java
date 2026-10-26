package com.surya.DistributedDataPlatform.jobs.service;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CsvValidationService {

    private static final long MAX_FILE_SIZE = 10 * 1024 * 1024;

    private static final Set<String> REQUIRED_HEADERS =
            Set.of("customerid", "name", "email");

    public void validate(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "CSV file must not be empty");
        }

        if (file.getSize() > MAX_FILE_SIZE) {
            throw new ResponseStatusException(
                    HttpStatus.PAYLOAD_TOO_LARGE,
                    "CSV file must not exceed 10 MB");
        }

        String filename = file.getOriginalFilename();

        if (filename == null ||
                !filename.toLowerCase(Locale.ROOT).endsWith(".csv")) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Only CSV files are accepted");
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(),
                        StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();

            if (headerLine == null || headerLine.isBlank()) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "CSV header is missing");
            }

            Set<String> headers = Arrays.stream(headerLine.split(",", -1))
                    .map(String::trim)
                    .map(h -> h.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toSet());

            if (!headers.containsAll(REQUIRED_HEADERS)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "CSV must contain customerId, name, and email headers");
            }
        } catch (IOException ex) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Could not read uploaded CSV", ex);
        }
    }
}
