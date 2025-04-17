package com.example.etl.service;

import com.example.etl.job.StepFactory;
import com.example.etl.model.EtlStep;
import com.example.etl.model.EtlTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service responsible for dynamically building and launching Spring Batch jobs
 * based on incoming EtlTaskConfig objects.
 */
@Service
public class DynamicJobService {

    private static final Logger log = LoggerFactory.getLogger(DynamicJobService.class);

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository; // Provided by @EnableBatchProcessing

    @Autowired
    private PlatformTransactionManager transactionManager; // Primary transaction manager

    @Autowired
    private StepFactory stepFactory; // Factory to create steps dynamically

    // Simple in-memory storage for task configurations accessible by step-scoped beans.
    // WARNING: Not suitable for production across multiple instances or restarts.
    // Replace with a persistent store (DB, Redis, etc.) keyed by configKey.
    private final Map<String, EtlTaskConfig> taskConfigRegistry = new ConcurrentHashMap<>();

    /**
     * Builds and launches a Spring Batch job based on the provided configuration.
     *
     * @param taskConfig The configuration defining the ETL task.
     * @throws Exception If job building or launching fails.
     */
    public void runJob(EtlTaskConfig taskConfig) throws Exception {

        String jobName = determineJobName(taskConfig);
        // Generate a unique ID for this specific execution attempt
        String uniqueRunId = UUID.randomUUID().toString().substring(0, 8);

        // Create a unique key to store/retrieve this task's config for this specific run
        String configKey = String.format("%s::%s::%s", jobName, taskConfig.getTaskId(), uniqueRunId);

        // Store config for retrieval within step-scoped beans
        taskConfigRegistry.put(configKey, taskConfig);
        log.debug("Registered task config with key: {}", configKey);

        // Build Job Parameters - Keep them simple string/long/double/date types.
        // Pass the configKey so steps can retrieve the full configuration.
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("taskId", taskConfig.getTaskId())
                .addString("configKey", configKey) // Key to retrieve full config
                .addDate("runDate", new Date()) // Ensures uniqueness for reruns if taskId is reused
                .addString("uniqueRunId", uniqueRunId) // Additional uniqueness
                // Add other simple parameters from config if useful (e.g., sourceTable)
                .addString("sourceSystem", taskConfig.getSourceType().name())
                .addString("targetSystem", taskConfig.getDestinationType().name())
                .toJobParameters();

        // --- Build the Job Dynamically ---
        log.info("Building dynamic job '{}' for configKey '{}'", jobName, configKey);
        JobBuilder jobBuilder = new JobBuilder(jobName, jobRepository);

        // Create the sequence of steps based on the config
        List<Step> stepsToExecute = taskConfig.getSteps().stream()
                .map(stepType -> {
                    try {
                        // Pass the transaction manager needed by the StepBuilder
                        return stepFactory.createStep(stepType, configKey, jobName, transactionManager);
                    } catch (Exception e) {
                        log.error("Failed to create step {} for job {}: {}", stepType, jobName, e.getMessage(), e);
                        // Return null or handle error appropriately (e.g., throw to prevent job launch)
                        return null; // Or throw new RuntimeException("Step creation failed for " + stepType, e);
                    }
                })
                .filter(step -> step != null) // Filter out steps that couldn't be created
                .collect(Collectors.toList());

        if (stepsToExecute.isEmpty()) {
            log.error("No valid steps could be created for task ID: {}. Cannot start job.", taskConfig.getTaskId());
            taskConfigRegistry.remove(configKey); // Clean up registry
            throw new IllegalArgumentException("No executable steps defined or created for the ETL task " + taskConfig.getTaskId());
        }

        // Build the job flow using the created steps
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>(jobName + "_flow");
        flowBuilder.start(stepsToExecute.get(0)); // Start with the first valid step
        for (int i = 1; i < stepsToExecute.size(); i++) {
            flowBuilder.next(stepsToExecute.get(i)); // Chain the subsequent steps
        }
        Flow flow = flowBuilder.build();

        // Assemble the final job definition
        Job job = jobBuilder.start(flow)
                .end()
                // Optional: Add Job listeners for logging start/end, cleanup, etc.
                .listener(new JobCompletionNotificationListener(this, configKey)) // Listener for cleanup
                .build();

        // --- Launch the Job ---
        log.info("Launching job '{}' with parameters: {}", jobName, jobParameters);
        try {
             // jobLauncher.run is asynchronous by default if using a TaskExecutor
            jobLauncher.run(job, jobParameters);
            log.info("Job '{}' launched successfully.", jobName);
        } catch (Exception e) {
            log.error("Failed to launch job '{}' with configKey '{}': {}", jobName, configKey, e.getMessage(), e);
            // Clean up the config from registry if launch fails immediately
            taskConfigRegistry.remove(configKey);
            throw e; // Re-throw exception to signal failure to the listener
        }
    }

    /**
     * Determines a descriptive and unique name for the job based on the task config.
     * Ensures the name is valid for Spring Batch job identification.
     *
     * @param config The ETL task configuration.
     * @return A sanitized job name string.
     */
    private String determineJobName(EtlTaskConfig config) {
        // Example: etl_ORACLE_DB_to_MSSQL_DB_LoadCustomerData
        String name = String.format("etl_%s_to_%s_%s",
                config.getSourceType(),
                config.getDestinationType(),
                config.getTaskId() // Include task ID for better identification in monitoring
        ).replaceAll("[^a-zA-Z0-9_\\-]", "_"); // Sanitize name for Batch compatibility
        // Ensure name isn't excessively long if taskId is very long
        return name.length() > 100 ? name.substring(0, 100) : name;
    }

    /**
     * Retrieves the EtlTaskConfig associated with a specific job run's configKey.
     * This is intended to be called by step-scoped beans during job execution.
     *
     * @param configKey The unique key identifying the job run's configuration.
     * @return The EtlTaskConfig.
     * @throws IllegalStateException if the configuration is not found (indicates a potential issue).
     */
    public EtlTaskConfig getTaskConfig(String configKey) {
        EtlTaskConfig config = taskConfigRegistry.get(configKey);
        if (config == null) {
            log.error("CRITICAL: EtlTaskConfig not found in registry for key: {}. This shouldn't happen during job execution.", configKey);
            // This usually indicates the config wasn't stored correctly or was prematurely removed.
            throw new IllegalStateException("Task configuration not found in registry for key: " + configKey);
        }
        return config;
    }

    /**
     * Removes the task configuration from the registry.
     * Should be called after a job completes (success or failure) to clean up memory.
     *
     * @param configKey The key of the configuration to remove.
     */
    public void removeTaskConfig(String configKey) {
        EtlTaskConfig removed = taskConfigRegistry.remove(configKey);
        if (removed != null) {
            log.debug("Removed task config from registry for key: {}", configKey);
        } else {
            log.warn("Attempted to remove task config for key {}, but it was not found.", configKey);
        }
    }
}
