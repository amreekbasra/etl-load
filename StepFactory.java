package com.example.etl.job;

import com.example.etl.model.EtlStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

/**
 * Factory component responsible for creating Spring Batch Step instances
 * based on the requested EtlStep type and associated configuration.
 */
@Component
public class StepFactory {

    private static final Logger log = LoggerFactory.getLogger(StepFactory.class);

    @Autowired
    private ApplicationContext applicationContext; // To get step-scoped beans if needed, though factories handle it now

    @Autowired
    private JobRepository jobRepository; // Required by StepBuilder

    // Inject factories for dynamic readers/writers
    @Autowired
    private ItemReaderFactory itemReaderFactory;
    @Autowired
    private ItemWriterFactory itemWriterFactory;

    // Inject tasklet beans (these should be @Component and @StepScope)
    @Autowired
    private TruncateTasklet truncateTasklet;
    @Autowired
    private NotifyTasklet notifyTasklet;
    // Add other tasklets (ValidateTasklet, ArchiveTasklet, etc.) here

    // Inject optional processor factory if needed
    // @Autowired private ItemProcessorFactory itemProcessorFactory;

    @Value("${etl.default.chunk.size:1000}")
    private int defaultChunkSize;

    /**
     * Creates a fully configured Spring Batch Step.
     *
     * @param stepType           The type of ETL step to create.
     * @param configKey          The unique key to retrieve the EtlTaskConfig for this job run.
     * @param jobName            The name of the parent job (used for step naming).
     * @param transactionManager The transaction manager to associate with the step.
     * @return A configured Step instance.
     * @throws IllegalArgumentException if the step type is unsupported or required components are missing.
     */
    public Step createStep(EtlStep stepType, String configKey, String jobName, PlatformTransactionManager transactionManager) {
        // Construct a unique and descriptive step name
        String stepName = String.format("%s_%s_step", jobName, stepType.name());
        StepBuilder stepBuilder = new StepBuilder(stepName, jobRepository);

        log.debug("Creating step '{}' of type [{}] for configKey '{}'", stepName, stepType, configKey);

        switch (stepType) {
            case TRUNCATE_DESTINATION:
                // Tasklet steps require the transaction manager passed to the tasklet method
                return stepBuilder.tasklet(truncateTasklet, transactionManager)
                        // Listener puts the configKey into the Step ExecutionContext
                        // so the @StepScope TruncateTasklet can access it via @Value
                        .listener(new StepContextParameterPutter("configKey", configKey))
                        .build();

            case LOAD:
                // Dynamically create reader and writer using their respective factories.
                // The factories return step-scoped beans configured using the configKey.
                ItemReader<Map<String, Object>> reader = itemReaderFactory.createReader(configKey);
                ItemWriter<Map<String, Object>> writer = itemWriterFactory.createWriter(configKey);

                // Optional: Create a processor if needed for transformations
                // ItemProcessor<Map<String, Object>, Map<String, Object>> processor = itemProcessorFactory.createProcessor(configKey);

                if (reader == null) {
                    throw new IllegalArgumentException("ItemReader could not be created for LOAD step in job " + jobName);
                }
                if (writer == null) {
                    throw new IllegalArgumentException("ItemWriter could not be created for LOAD step in job " + jobName);
                }

                // Build the chunk-oriented step
                // <I, O> - Input type from Reader, Output type to Writer
                var chunkStepBuilder = stepBuilder.<Map<String, Object>, Map<String, Object>>chunk(defaultChunkSize, transactionManager)
                        .reader(reader)
                        // .processor(processor) // Uncomment if using a processor
                        .writer(writer);

                // Add common step configurations (listeners, fault tolerance)
                chunkStepBuilder.listener(new StepContextParameterPutter("configKey", configKey)); // Ensure key is available

                // Example Fault Tolerance (customize as needed)
                // chunkStepBuilder.faultTolerant()
                //     .retryLimit(3).retry(java.net.SocketTimeoutException.class) // Retry specific exceptions
                //     .skipLimit(10).skip(org.springframework.dao.DataIntegrityViolationException.class); // Skip others

                return chunkStepBuilder.build();

            case NOTIFY_SUCCESS:
            case NOTIFY_FAILURE: // The tasklet itself can check job status if needed
                 return stepBuilder.tasklet(notifyTasklet, transactionManager)
                        .listener(new StepContextParameterPutter("configKey", configKey))
                        .build();

            // Add cases for other EtlStep enum values (VALIDATE_SOURCE, VALIDATE_LOAD, etc.)
            // using appropriate Tasklets or chunk steps.

            default:
                log.error("Unsupported ETL step type provided: {}", stepType);
                throw new IllegalArgumentException("Unsupported step type: " + stepType);
        }
    }
}
