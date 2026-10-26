package com.surya.DistributedDataPlatform.common.exception;

import java.util.UUID;

public class IngestionJobNotFoundException extends RuntimeException {

    public IngestionJobNotFoundException(UUID id) {
        super("Ingestion job " + id + " was not found");
    }
}
