# System Architecture

## Overview

The **Distributed Data Ingestion & Processing Platform** is an
asynchronous CSV ingestion system built with Java, Spring Boot, Apache
Kafka, and PostgreSQL.

It accepts CSV uploads, validates data against a selected destination
table, and processes ingestion jobs asynchronously. The platform uses a
transactional outbox to publish job events, Kafka consumers to process
those events, and PostgreSQL to persist job state and ingestion results.

## High-Level Architecture

``` mermaid
flowchart TB
    Client["Client / API Consumer"]
    API["Spring Boot REST API"]
    Upload["CSV Upload"]
    Job["Ingestion Job Service"]
    DB[("PostgreSQL")]
    Outbox["Transactional Outbox"]
    Publisher["Outbox Publisher"]
    Kafka[["Kafka: ingestion-jobs"]]
    Consumer["Kafka Ingestion Consumer"]
    Processed["Processed Event Service"]
    Batch["Batch Processing Service"]
    Validation["Dynamic CSV Validation"]
    Writer["Dynamic JDBC Batch Writer"]
    Destination[("Destination Tables")]
    Ledger[("Dynamic Ingestion Row Ledger")]
    DLT[["Kafka: ingestion-jobs-dlt"]]
    DLTConsumer["DLT Consumer"]
    DLTService["Processed DLT Event Service"]
    Actuator["Spring Boot Actuator"]

    Client --> API
    API --> Upload
    Upload --> Job
    Job --> DB
    Job --> Outbox
    Outbox --> Publisher
    Publisher --> Kafka
    Kafka --> Consumer
    Consumer --> Processed
    Consumer --> Batch
    Batch --> Validation
    Validation --> Writer
    Writer --> Destination
    Batch --> DB
    Batch --> Ledger
    Kafka -->|"Retries exhausted"| DLT
    DLT --> DLTConsumer
    DLTConsumer --> DLTService
    DLTService --> DB
    API --> Actuator
```

## Components

### 1. REST API and CSV Upload

The Spring Boot API exposes the ingestion-job upload endpoint. A request
supplies a CSV file together with the destination schema and table.

### 2. Ingestion Job Service

The service creates and persists an ingestion job. Job information and
its outbox event are stored transactionally so that the event can be
published asynchronously.

### 3. Transactional Outbox and Publisher

The outbox publisher reads pending outbox events and publishes ingestion
events to the `ingestion-jobs` Kafka topic. This separates job creation
from asynchronous message delivery.

### 4. Kafka Ingestion Consumer

The consumer receives ingestion events and delegates processing to the
batch-processing service. The processed-event service records event
claims and completion state to prevent duplicate processing.

### 5. Batch Processing and Dynamic Validation

The batch-processing service reads the uploaded CSV and coordinates
validation and persistence. Dynamic CSV validation checks incoming
headers and values against destination-table metadata rather than
relying on a fixed CSV schema.

### 6. Dynamic JDBC Batch Writer

The JDBC writer persists accepted rows to the requested destination
table. The platform also records ingestion outcomes and row-level
processing information in PostgreSQL.

### 7. Retry and Dead Letter Topic

The Kafka listener uses an error handler with a dead-letter publishing
recoverer. After retry handling is exhausted, the failed event is routed
to `ingestion-jobs-dlt`. The DLT consumer delegates handling to the
processed-DLT-event service, which records handled event IDs and marks
the associated job as failed.

### 8. PostgreSQL

PostgreSQL stores ingestion jobs, outbox events, processed-event
records, DLT processing records, and ingestion data/row-ledger
information.

### 9. Health and Monitoring

Spring Boot Actuator exposes application health information, including
database and Kafka health indicators when configured.

## End-to-End Processing Flow

1.  A client submits a CSV file with a destination schema and table.
2.  The API creates an ingestion job and a corresponding outbox event.
3.  The outbox publisher publishes the event to Kafka.
4.  The Kafka consumer receives the event and attempts to claim it for
    processing.
5.  The batch-processing service validates the CSV against destination
    metadata.
6.  Valid rows are written to the destination table through the JDBC
    batch writer.
7.  Job and row-processing results are persisted in PostgreSQL.
8.  On processing failure, the consumer releases the event claim and
    propagates the exception so Kafka error handling can retry delivery.
9.  If retries are exhausted, the event is published to the DLT and
    handled by the DLT consumer.

## Technology Stack

  Area                    Technology
  ----------------------- ---------------------------------------------------
  Language                Java 25
  Application framework   Spring Boot 4
  Messaging               Apache Kafka 4.1
  Database                PostgreSQL 17
  Persistence             Spring Data JPA and JDBC
  Database migrations     Flyway
  Build                   Gradle
  Containerization        Docker Compose
  Health monitoring       Spring Boot Actuator
  Testing                 JUnit and the project's configured test libraries

## Key Engineering Concepts

-   **Transactional outbox:** persists the job and event record together
    before asynchronous publication.
-   **Idempotent event processing:** uses processed-event records to
    avoid processing the same event more than once.
-   **Dynamic schema-aware validation:** validates CSV content using
    destination-table metadata.
-   **JDBC batch writing:** writes accepted records using JDBC batch
    operations.
-   **Retry and dead-letter handling:** retries failed Kafka deliveries
    and routes exhausted failures to a dedicated DLT.
-   **Database migrations:** manages schema evolution through Flyway.

## Local Services

The Docker Compose setup runs the application, PostgreSQL, and Kafka.

  Service                   Host access
  ------------------------- -------------------------
  Spring Boot application   `http://localhost:8081`
  PostgreSQL                `localhost:5433`
  Kafka broker              `localhost:9093`

The application uses the internal Docker network addresses when
connecting to PostgreSQL and Kafka from inside the Compose environment.

## Notes

This document describes the implemented architecture at a component
level. Operational details such as exact retry counts, batch sizes, and
deployment topology should be documented separately and kept aligned
with the active application configuration.
