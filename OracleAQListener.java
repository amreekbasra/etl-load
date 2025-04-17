package com.example.etl.listener;

import com.example.etl.model.EtlTaskConfig;
import com.example.etl.service.DynamicJobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

/**
 * Listens to the configured Oracle AQ queue for incoming ETL task messages.
 */
@Component
public class OracleAQListener {

    private static final Logger log = LoggerFactory.getLogger(OracleAQListener.class);

    @Autowired
    private ObjectMapper objectMapper; // Spring Boot auto-configures this

    @Autowired
    private DynamicJobService dynamicJobService;

    /**
     * Receives messages from the ETL task queue.
     * Uses CLIENT_ACKNOWLEDGE mode, requiring manual acknowledgment.
     *
     * @param message The received JMS message.
     * @param session The JMS session (useful for recover).
     */
    @JmsListener(destination = "${etl.queue.name}", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessage(Message message, Session session) {
        String messageId = "N/A";
        try {
            messageId = message.getJMSMessageID();
            log.info("Received message ID: {}", messageId);

            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String payload = textMessage.getText();
                log.debug("Message Payload for ID [{}]: {}", messageId, payload);

                // 1. Parse the message payload
                EtlTaskConfig taskConfig = objectMapper.readValue(payload, EtlTaskConfig.class);

                // Basic validation - ensure critical info is present
                if (taskConfig.getTaskId() == null || taskConfig.getTaskId().isBlank()) {
                    // Generate a default Task ID if missing, based on JMS Message ID
                    taskConfig.setTaskId("task_" + messageId.replaceAll("[^a-zA-Z0-9_\\-]", ""));
                    log.warn("Task ID was missing in message [{}], generated: {}", messageId, taskConfig.getTaskId());
                }
                if (taskConfig.getSourceType() == null || taskConfig.getDestinationType() == null || taskConfig.getSteps() == null || taskConfig.getSteps().isEmpty()) {
                     throw new IllegalArgumentException("Invalid task configuration received: Missing source/destination type or steps.");
                }

                log.info("Parsed ETL Task Config for Task ID [{}], Source [{}], Dest [{}], Steps [{}]",
                         taskConfig.getTaskId(), taskConfig.getSourceType(), taskConfig.getDestinationType(), taskConfig.getSteps());

                // 2. Launch the dynamic Spring Batch job
                // This call should be relatively quick; the job runs asynchronously.
                dynamicJobService.runJob(taskConfig);
                log.info("Successfully launched job for Task ID [{}] from message ID [{}]", taskConfig.getTaskId(), messageId);

                // 3. Acknowledge the message ONLY after successful job launch.
                // This removes the message from the queue. If launch fails, message remains for redelivery/DLQ.
                message.acknowledge();
                log.info("Message acknowledged successfully for ID: {}", messageId);

            } else {
                log.warn("Received message ID [{}] of unexpected type: {}. Discarding.", messageId, message.getClass().getName());
                // Acknowledge non-text messages to remove them if we cannot process them.
                message.acknowledge();
            }

        } catch (Exception e) {
            // Log different levels based on expected vs unexpected errors
            if (e instanceof com.fasterxml.jackson.core.JsonProcessingException) {
                log.error("JSON Parsing Error for message ID [{}]: {}. Message likely malformed. Consider DLQ.", messageId, e.getMessage());
            } else if (e instanceof IllegalArgumentException) {
                 log.error("Invalid ETL Configuration Error for message ID [{}]: {}. Check message content. Consider DLQ.", messageId, e.getMessage());
            } else if (e instanceof org.springframework.batch.core.JobParametersInvalidException ||
                       e instanceof org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException || // May happen if taskId isn't unique enough with runDate
                       e instanceof org.springframework.batch.core.repository.JobRestartException) {
                log.error("Batch Job Launch Error for message ID [{}]: {}. Review Job Parameters/State. Consider DLQ.", messageId, e.getMessage());
            }
             else {
                log.error("Unexpected Error processing message ID [{}]: {}. Recovery may be needed.", messageId, e.getMessage(), e);
            }

            // CRITICAL: Handling Acknowledgement on Failure with CLIENT_ACKNOWLEDGE
            // DO NOT ACKNOWLEDGE here if you want the message to be redelivered (based on AQ config)
            // or eventually moved to an AQ Exception Queue (DLQ).
            // Acknowledging here means the message is permanently lost despite the processing failure.
            log.warn("Message ID [{}] processing failed. NOT acknowledging message to allow redelivery/DLQ.", messageId);

            // Optional: Trigger session recovery for immediate redelivery attempt (use with caution)
            // try {
            //     log.warn("Attempting session recovery for redelivery of message ID [{}]", messageId);
            //     session.recover();
            // } catch (JMSException recoverEx) {
            //     log.error("Failed to recover JMS session for message ID [{}]: {}", messageId, recoverEx.getMessage(), recoverEx);
            // }
        }
    }
}
