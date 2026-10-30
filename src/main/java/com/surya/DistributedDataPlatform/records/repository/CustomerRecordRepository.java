package com.surya.DistributedDataPlatform.records.repository;

import com.surya.DistributedDataPlatform.records.entity.CustomerRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface CustomerRecordRepository
        extends JpaRepository<CustomerRecord, UUID> {
}