package com.example.etl.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

/**
 * A simple StepExecutionListener used to put a specific key-value pair
 * into the Step ExecutionContext *before* the step starts.
 * This allows step-scoped beans (like Tasklets or ItemReaders/Writers created by factories)
 * to access job-specific parameters (like the configKey) via @Value("#{stepExecutionContext['key']}").
 */
public class StepContextParameterPutter implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(StepContextParameterPutter.class);

    private final String key;
    private final Object value;

    /**
     * Creates the listener.
     * @param key The key to put into the execution context.
     * @param value The value to associate with the key.
     */
    public StepContextParameterPutter(String key, Object value) {
        if (key == null || value == null) {
            // Prevent NullPointerException later
            throw new IllegalArgumentException("Key and Value for StepContextParameterPutter cannot be null.");
        }
        this.key = key;
        this.value = value;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.debug("Putting key='{}', value='{}' into StepExecutionContext for step '{}'", key, value, stepExecution.getStepName());
        // Put the key-value pair into the Step's ExecutionContext
        stepExecution.getExecutionContext().put(key, value);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        // No action needed after the step for this listener.
        // Return null to indicate no change to the exit status.
        return null;
    }
}
