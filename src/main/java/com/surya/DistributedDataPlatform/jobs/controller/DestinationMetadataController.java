package com.surya.DistributedDataPlatform.jobs.controller;

import com.surya.DistributedDataPlatform.jobs.dto.DestinationTableMetadata;
import com.surya.DistributedDataPlatform.jobs.service.PostgreSqlDestinationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/destinations")
public class DestinationMetadataController {

    private final PostgreSqlDestinationService destinationService;

    public DestinationMetadataController(
            PostgreSqlDestinationService destinationService
    ) {
        this.destinationService = destinationService;
    }

    @GetMapping("/{schemaName}/{tableName}/metadata")
    public DestinationTableMetadata getTableMetadata(
            @PathVariable String schemaName,
            @PathVariable String tableName
    ) {
        return destinationService.getTableMetadata(
                schemaName,
                tableName
        );
    }
}