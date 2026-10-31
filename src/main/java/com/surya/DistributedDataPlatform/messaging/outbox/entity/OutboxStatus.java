package com.surya.DistributedDataPlatform.messaging.outbox.entity;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    PUBLISHED,
    FAILED
}