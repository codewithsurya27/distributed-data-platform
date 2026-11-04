package com.surya.DistributedDataPlatform.jobs.controller;

import com.surya.DistributedDataPlatform.jobs.dto.CreateIngestionJobRequest;
import com.surya.DistributedDataPlatform.jobs.dto.CsvUploadResponse;
import com.surya.DistributedDataPlatform.jobs.dto.IngestionJobResponse;
import com.surya.DistributedDataPlatform.jobs.service.CsvStorageService;
import com.surya.DistributedDataPlatform.jobs.service.CsvValidationService;
import com.surya.DistributedDataPlatform.jobs.service.IngestionJobService;
import com.surya.DistributedDataPlatform.jobs.service.PostgreSqlDestinationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1/ingestion-jobs")
public class CsvUploadController {

    private static final Logger log =
            LoggerFactory.getLogger(CsvUploadController.class);

    private final CsvValidationService validationService;
    private final CsvStorageService storageService;
    private final IngestionJobService jobService;
    private final PostgreSqlDestinationService destinationService;

    public CsvUploadController(
            CsvValidationService validationService,
            CsvStorageService storageService,
            IngestionJobService jobService,
            PostgreSqlDestinationService destinationService
    ) {
        this.validationService = validationService;
        this.storageService = storageService;
        this.jobService = jobService;
        this.destinationService = destinationService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<CsvUploadResponse> upload(
            @RequestParam("file") MultipartFile file,
            @RequestParam("schema") String schema,
            @RequestParam("table") String table
    ) {

        // Validate the destination before storing the uploaded file.
        destinationService.validateDestination(schema, table);

        // Validate uploaded CSV structure.
        validationService.validate(file);

        String filePath = null;

        try {
            filePath = storageService.store(file);

            CreateIngestionJobRequest request =
                    new CreateIngestionJobRequest(
                            file.getOriginalFilename(),
                            filePath,
                            schema,
                            table
                    );

            IngestionJobResponse job = jobService.create(request);

            log.info(
                    "Accepted CSV upload for ingestion job {} targeting {}.{} (sizeBytes={})",
                    job.id(),
                    schema,
                    table,
                    file.getSize()
            );

            CsvUploadResponse response = new CsvUploadResponse(
                    job.id(),
                    file.getOriginalFilename(),
                    "PENDING",
                    "File uploaded successfully"
            );

            return ResponseEntity.accepted().body(response);

        } catch (IOException ex) {

            log.error(
                    "Could not store uploaded CSV (sizeBytes={})",
                    file.getSize(),
                    ex
            );

            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Could not store uploaded CSV",
                    ex
            );

        } catch (RuntimeException ex) {

            log.error(
                    "Could not create ingestion job for uploaded CSV; cleaning up stored file",
                    ex
            );

            if (filePath != null) {
                try {
                    storageService.delete(filePath);
                } catch (IOException cleanupException) {
                    log.warn(
                            "Could not remove uploaded CSV after job creation failed",
                            cleanupException
                    );
                    ex.addSuppressed(cleanupException);
                }
            }

            throw ex;
        }
    }
}