package com.example.etl.job;

import com.example.etl.model.EtlTaskConfig;
import com.example.etl.service.DynamicJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

/**
 * A Spring Batch Tasklet that performs notification actions based on the ETL job outcome.
 * Must be step-scoped to access job/step context.
 */
@Component // Make it a Spring bean
@StepScope   // Crucial: Creates a new instance for each step execution
public class NotifyTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(NotifyTasklet.class);

    @Autowired
    private DynamicJobService dynamicJobService; // Service to retrieve the full task config

    // Inject the configKey from the step execution context
    private final String configKey;

    // Inject notification service beans here if needed (e.g., EmailService, SlackService)
    // @Autowired private EmailService emailService;

    // Constructor injection for step-scoped values
    public NotifyTasklet(@Value("#{stepExecutionContext['configKey']}") String configKey) {
        this.configKey = configKey;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Executing NotifyTasklet for configKey: {}", configKey);

        if (!StringUtils.hasText(configKey)) {
            log.error("ConfigKey is null or empty in NotifyTasklet. Cannot proceed.");
            throw new IllegalStateException("configKey not found in step execution context for NotifyTasklet.");
        }

        EtlTaskConfig config = dynamicJobService.getTaskConfig(configKey);
        Map<String, String> notificationDetails = config.getNotificationDetails();

        // Determine the overall job status to decide if it's a success/failure notification
        ExitStatus jobExitStatus = chunkContext.getStepContext().getStepExecution().getJobExecution().getExitStatus();
        boolean isSuccess = ExitStatus.COMPLETED.equals(jobExitStatus); // Basic check, might need refinement

        String statusString = isSuccess ? "SUCCESS" : "FAILURE (" + jobExitStatus.getExitCode() + ")";

        // Construct the notification message
        String message = String.format("ETL Task '%s' (%s -> %s) finished. Final Status: %s. JobExecutionId: %d.",
                config.getTaskId(),
                config.getSourceType(),
                config.getDestinationType(),
                statusString,
                chunkContext.getStepContext().getStepExecution().getJobExecutionId()
        );

        // --- Placeholder for Actual Notification Logic ---
        log.warn("--- NOTIFICATION --- : {}", message);
        if (notificationDetails != null) {
            String emailTo = notificationDetails.get("emailTo");
            if (StringUtils.hasText(emailTo)) {
                 log.info("Attempting to send notification email to: {}", emailTo);
                 // try {
                 //     emailService.sendNotification(emailTo, "ETL Job Status: " + config.getTaskId(), message);
                 // } catch (Exception e) {
                 //     log.error("Failed to send notification email for task {}: {}", config.getTaskId(), e.getMessage());
                 //     // Decide if notification failure should fail the step
                 //     // contribution.setExitStatus(ExitStatus.FAILED); // Example
                 // }
            }
            // Add logic for other notification channels (Slack, Webhook, etc.)
        } else {
             log.info("No notification details configured for task {}.", config.getTaskId());
        }
        // --- End Placeholder ---

        // Indicate that this tasklet is finished
        return RepeatStatus.FINISHED;
    }
}
