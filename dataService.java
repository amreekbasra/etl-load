package com.example.etl.job;

import com.example.etl.model.EtlStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor; // Import if using processor
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

/**
 * Factory component responsible for creating Spring Batch Step instances.
 * The LOAD step created here orchestrates the data handling (Read -> Process -> Write).
 */
@Component
public class StepFactory {

    private static final Logger log = LoggerFactory.getLogger(StepFactory.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private JobRepository jobRepository; // Required by StepBuilder

    // Inject factories for dynamic readers/writers
    @Autowired
    private ItemReaderFactory itemReaderFactory; // Builds the "FetchDataRepo"
    @Autowired
    private ItemWriterFactory itemWriterFactory; // Builds the "InsertBulkDataRepo"

    // Inject tasklet beans (these should be @Component and @StepScope)
    @Autowired
    private TruncateTasklet truncateTasklet;
    @Autowired
    private NotifyTasklet notifyTasklet;
    // Add other tasklets (ValidateTasklet, ArchiveTasklet, etc.) here

    // Inject optional processor factory if needed
    // @Autowired private ItemProcessorFactory itemProcessorFactory;

    @Value("${etl.default.chunk.size:1000}") // Chunk size from properties
    private int defaultChunkSize;

    /**
     * Creates a fully configured Spring Batch Step based on the EtlStep type.
     * The LOAD step demonstrates the data handling orchestration.
     *
     * @param stepType           The type of ETL step to create.
     * @param configKey          The unique key to retrieve the EtlTaskConfig for this job run.
     * @param jobName            The name of the parent job (used for step naming).
     * @param transactionManager The transaction manager to associate with the step.
     * @return A configured Step instance.
     * @throws IllegalArgumentException if the step type is unsupported or required components are missing.
     */
    public Step createStep(EtlStep stepType, String configKey, String jobName, PlatformTransactionManager transactionManager) {
        String stepName = String.format("%s_%s_step", jobName, stepType.name());
        StepBuilder stepBuilder = new StepBuilder(stepName, jobRepository);

        log.debug("Creating step '{}' of type [{}] for configKey '{}'", stepName, stepType, configKey);

        switch (stepType) {
            case TRUNCATE_DESTINATION:
                // Tasklet steps handle simple, non-chunk operations
                return stepBuilder.tasklet(truncateTasklet, transactionManager)
                        .listener(new StepContextParameterPutter("configKey", configKey))
                        .build();

            case LOAD:
                // --- This is the core Data Handling Orchestration ---
                log.debug("Creating LOAD step (Data Handling Orchestration): {}", stepName);

                // 1. Get the Reader (Data Fetcher)
                ItemReader<Map<String, Object>> reader = itemReaderFactory.createReader(configKey);

                // 2. Get the Writer (Bulk Inserter)
                ItemWriter<Map<String, Object>> writer = itemWriterFactory.createWriter(configKey);

                // 3. Optional: Get a Processor (for transformations)
                // ItemProcessor<Map<String, Object>, Map<String, Object>> processor = itemProcessorFactory.createProcessor(configKey);

                if (reader == null || writer == null) {
                    log.error("Cannot create LOAD step {}. Reader or Writer could not be created.", stepName);
                    throw new IllegalArgumentException("Reader or Writer creation failed for LOAD step in job " + jobName);
                }

                // 4. Build the Chunk Step - Spring Batch handles the loop:
                //    - Read items until chunk size is reached or reader returns null.
                //    - Pass the chunk to the processor (if defined).
                //    - Pass the (processed) chunk to the writer.
                //    - Commit transaction.
                var chunkStepBuilder = stepBuilder.<Map<String, Object>, Map<String, Object>>chunk(defaultChunkSize, transactionManager)
                        .reader(reader) // Input: Fetched data
                        // .processor(processor) // Optional: Transform data
                        .writer(writer); // Output: Bulk insert data

                // Add listener to ensure step-scoped beans can get the configKey
                chunkStepBuilder.listener(new StepContextParameterPutter("configKey", configKey));

                // Add Fault Tolerance (Retry/Skip) as needed for enterprise robustness
                // chunkStepBuilder.faultTolerant()
                //     .retryLimit(3).retry(java.net.SocketTimeoutException.class)
                //     .skipLimit(10).skip(org.springframework.dao.DataIntegrityViolationException.class);

                return chunkStepBuilder.build();
                // --- End of Data Handling Orchestration for LOAD ---

            case NOTIFY_SUCCESS:
            case NOTIFY_FAILURE:
                 return stepBuilder.tasklet(notifyTasklet, transactionManager)
                        .listener(new StepContextParameterPutter("configKey", configKey))
                        .build();

            // Add cases for other EtlStep enum values

            default:
                log.error("Unsupported ETL step type provided: {}", stepType);
                throw new IllegalArgumentException("Unsupported step type: " + stepType);
        }
    }
}
