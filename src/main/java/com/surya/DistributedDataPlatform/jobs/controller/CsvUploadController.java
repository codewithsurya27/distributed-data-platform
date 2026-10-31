package com.surya.DistributedDataPlatform.jobs.controller;

import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.CsvUploadResponse;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.service.CsvStorageService;
import com.surya.DistributedDataPlatform.jobs.service.CsvValidationService;
import com.surya.DistributedDataPlatform.jobs.service.IngestionJobService;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1/ingestion-jobs")
public class CsvUploadController {

    private final CsvValidationService validationService;
    private final CsvStorageService storageService;
    private final IngestionJobService jobService;

    public CsvUploadController(
            CsvValidationService validationService,
            CsvStorageService storageService,
            IngestionJobService jobService) {

        this.validationService = validationService;
        this.storageService = storageService;
        this.jobService = jobService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<CsvUploadResponse> upload(
            @RequestPart("file") MultipartFile file) {

        // Validate uploaded CSV.
        validationService.validate(file);

        String filePath = null;

        try {
            // Store CSV file.
            filePath = storageService.store(file);

            // Create job and outbox event in one DB transaction.
            CreateIngestionJobRequest request =
                    new CreateIngestionJobRequest(
                            file.getOriginalFilename(),
                            filePath
                    );

            IngestionJobResponse job = jobService.create(request);

            CsvUploadResponse response = new CsvUploadResponse(
                    job.id(),
                    file.getOriginalFilename(),
                    "PENDING",
                    "File uploaded successfully"
            );

            return ResponseEntity.accepted().body(response);

        } catch (IOException ex) {

            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Could not store uploaded CSV",
                    ex
            );

        } catch (RuntimeException ex) {

            // Remove uploaded file if job creation fails.
            if (filePath != null) {
                try {
                    storageService.delete(filePath);
                } catch (IOException cleanupException) {
                    ex.addSuppressed(cleanupException);
                }
            }

            throw ex;
        }
    }
}
