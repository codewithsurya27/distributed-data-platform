package com.surya.DistributedDataPlatform.processing.worker;

import com.surya.DistributedDataPlatform.jobs.entity.IngestionJob;
import com.surya.DistributedDataPlatform.jobs.entity.IngestionJobStatus;
import com.surya.DistributedDataPlatform.jobs.repository.IngestionJobRepository;
import com.surya.DistributedDataPlatform.processing.service.BatchProcessingService;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class IngestionWorker {

    private final IngestionJobRepository jobRepository;
    private final BatchProcessingService processingService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public IngestionWorker(
            IngestionJobRepository jobRepository,
            BatchProcessingService processingService) {
        this.jobRepository = jobRepository;
        this.processingService = processingService;
    }

    @Scheduled(fixedDelayString = "${app.processing.poll-delay-ms:2000}")
    public void pollPendingJobs() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try {
            List<IngestionJob> jobs =
                    jobRepository.findByStatusOrderByCreatedAtAsc(
                            IngestionJobStatus.PENDING);

            if (!jobs.isEmpty()) {
                processingService.process(jobs.get(0).getId());
            }
        } finally {
            running.set(false);
        }
    }
}