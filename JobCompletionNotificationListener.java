package com.example.etl.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

/**
 * Listener attached to the dynamic job to perform actions after job completion,
 * such as cleaning up the configuration from the registry.
 */
public class JobCompletionNotificationListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final DynamicJobService dynamicJobService;
    private final String configKey;

    /**
     * Constructor to inject dependencies.
     * @param dynamicJobService Service to call for cleanup.
     * @param configKey The specific config key associated with the job this listener is attached to.
     */
    public JobCompletionNotificationListener(DynamicJobService dynamicJobService, String configKey) {
        this.dynamicJobService = dynamicJobService;
        this.configKey = configKey;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("JOB '{}' STARTING with configKey: {}", jobExecution.getJobInstance().getJobName(), configKey);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        BatchStatus status = jobExecution.getStatus();

        log.info("JOB '{}' FINISHED with Status: [{}] for configKey: {}", jobName, status, configKey);

        // Always attempt to clean up the configuration from the registry after the job finishes.
        try {
            log.debug("Attempting to remove task config from registry for completed job. Key: {}", configKey);
            dynamicJobService.removeTaskConfig(configKey);
        } catch (Exception e) {
            log.error("Error removing task config from registry for key {} after job completion: {}", configKey, e.getMessage(), e);
            // Log the error but don't prevent job completion status logging.
        }

        if (status == BatchStatus.COMPLETED) {
            log.info("Job '{}' completed successfully.", jobName);
            // Add any success-specific logic here (e.g., final notification)
        } else if (status == BatchStatus.FAILED) {
            log.error("Job '{}' failed. Review logs for details. Parameters: {}", jobName, jobExecution.getJobParameters());
            // Add failure-specific logic here (e.g., failure notification)
            jobExecution.getAllFailureExceptions().forEach(
                ex -> log.error("Failure Exception in Job '{}': {}", jobName, ex.getMessage(), ex)
            );
        } else {
             log.warn("Job '{}' finished with status: {}", jobName, status);
        }
    }
}
