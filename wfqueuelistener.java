// --- EtlWorkflowEngineApplication.java ---
package com.yourcompany.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableKafka // Enable Kafka listener container factory
@EnableAsync // Enable asynchronous execution for jobs
public class EtlWorkflowEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EtlWorkflowEngineApplication.class, args);
    }
}

// --- model/JobConfig.java (Partial - needs fields matching JSON) ---
package com.yourcompany.etl.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Data; // Using Lombok for brevity

@Data // Generates getters, setters, toString, etc.
@JsonIgnoreProperties(ignoreUnknown = true) // Tolerate extra fields in JSON
public class JobConfig {
    private String jobId;
    private int priority;
    private SourceConfig source;
    private DestinationConfig destination;
    private List<Mapping> mappings;
    private ErrorHandling errorHandling;
    private Transformation transformation;
    private Monitoring monitoring;

    // Inner classes for nested structures (SourceConfig, DestinationConfig, etc.)
    // Example:
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SourceConfig {
        private String type; // e.g., ORACLE_DB, FLAT_FILE
        private Map<String, Object> connectionDetails;
    }

     @Data
     @JsonIgnoreProperties(ignoreUnknown = true)
     public static class DestinationConfig {
         private String type; // e.g., ORACLE_DB, FLAT_FILE, ELASTICSEARCH
         private Map<String, Object> connectionDetails;
         private int batchSize = 1000; // Default batch size
     }

     @Data
     @JsonIgnoreProperties(ignoreUnknown = true)
     public static class Mapping {
        private String sourceFieldName;
        private String destinationFieldName;
        private String sourceFieldType;
        private String destFieldType;
        private boolean isSourceNullable;
        private boolean isDestNullable;
     }

     @Data
     @JsonIgnoreProperties(ignoreUnknown = true)
     public static class ErrorHandling {
         private String strategy = "FAIL_JOB"; // e.g., FAIL_JOB, ROUTE_TO_FILE, LOG_ONLY
         private String errorFilePath;
         private long maxErrorsAllowed = 0; // 0 means fail on first error if strategy is FAIL_JOB
     }

     @Data
     @JsonIgnoreProperties(ignoreUnknown = true)
     public static class Transformation {
         private String type = "NONE"; // e.g., NONE, PYTHON_SCRIPT
         private String scriptPath;
         private Map<String, Object> parameters;
     }

      @Data
      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Monitoring {
          private long progressUpdateFrequency = 10000; // Default progress update frequency
      }

}

// --- model/JobStatus.java ---
package com.yourcompany.etl.model;

public enum JobStatus {
    UNKNOWN,
    SUBMITTED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED // Added for potential cancellation feature
}

// --- listener/WorkflowQueueListener.java ---
package com.yourcompany.etl.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourcompany.etl.model.JobConfig;
import com.yourcompany.etl.service.WorkflowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class WorkflowQueueListener {

    private static final Logger log = LoggerFactory.getLogger(WorkflowQueueListener.class);

    // Use Spring Boot's auto-configured ObjectMapper
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private WorkflowManager workflowManager;

    // Configure topic and group ID in application.yml
    @KafkaListener(topics = "${etl.kafka.topic.workflow}", groupId = "${etl.kafka.group-id}")
    public void listen(@Payload String message) {
        JobConfig jobConfig = null;
        String jobIdForLogging = "UNKNOWN";
        try {
            log.debug("Received raw message: {}", message); // Log raw message at debug level
            jobConfig = objectMapper.readValue(message, JobConfig.class);
            jobIdForLogging = jobConfig.getJobId(); // Get jobId for logging context

            // Put jobId in logging context for traceability across service calls
            MDC.put("jobId", jobIdForLogging);
            log.info("Parsed job configuration received for Job ID: {}", jobIdForLogging);

            // Submit job for asynchronous execution
            workflowManager.submitJob(jobConfig);

        } catch (Exception e) {
            // Handle parsing errors or initial submission failures
            MDC.put("jobId", jobIdForLogging); // Ensure jobId is in MDC for error logging
            log.error("Failed to process Kafka message or submit job for Job ID [{}]: {}", jobIdForLogging, message, e);
            // TODO: Implement dead-letter queue mechanism for unparseable messages
            // Consider sending the raw message to a DLT (Dead Letter Topic)
        } finally {
            MDC.remove("jobId"); // Clean up MDC for this thread
        }
    }
}

// --- service/WorkflowManager.java ---
package com.yourcompany.etl.service;

import com.yourcompany.etl.core.EtlJob;
import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.JobProgressListener;
import com.yourcompany.etl.core.factory.DataReaderFactory;
import com.yourcompany.etl.core.factory.DataWriterFactory;
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.JobStatus; // Use core model
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class WorkflowManager implements JobProgressListener {

    private static final Logger log = LoggerFactory.getLogger(WorkflowManager.class);

    // Use factories from the core module
    @Autowired
    private DataReaderFactory readerFactory;
    @Autowired
    private DataWriterFactory writerFactory;
    @Autowired
    private KibanaLogService kibanaLogService; // Service to push structured logs/status

    // Track running jobs, their status, and progress
    // Use AtomicReference for status to ensure atomic updates if needed elsewhere
    private final Map<String, AtomicReference<JobStatus>> jobStatuses = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> jobProgressCounters = new ConcurrentHashMap<>();
    private final Map<String, String> jobStatusMessages = new ConcurrentHashMap<>(); // Store last message

    // Resource Monitoring Thresholds (configurable via application.yml)
    @Value("${etl.engine.monitoring.max-heap-usage-percent:85}")
    private double maxHeapUsagePercent;

    @Value("${etl.engine.monitoring.max-cpu-load-percent:90}")
    private double maxCpuLoadPercent;

    // Submit job asynchronously using Spring's @Async
    // Requires a TaskExecutor bean configured (e.g., ThreadPoolTaskExecutor)
    @Async("jobExecutor") // Reference the configured TaskExecutor bean name
    public CompletableFuture<Void> submitJob(JobConfig config) {
        String jobId = config.getJobId();
        MDC.put("jobId", jobId); // Set MDC context for the execution thread

        log.info("Attempting to start job execution: {}", jobId);

        if (!canStartJob(jobId)) {
             MDC.remove("jobId");
             return CompletableFuture.completedFuture(null); // Indicate job didn't start now
        }

        updateJobStatus(jobId, JobStatus.RUNNING, "Job execution starting.");
        kibanaLogService.logJobStatus(jobId, JobStatus.RUNNING, "Execution starting", 0.0);
        jobProgressCounters.put(jobId, new AtomicLong(0));

        try {
            // Create context specific to this job run
            JobContext context = new JobContext(jobId, this, config, LoggerFactory.getLogger("ETLJob." + jobId));

            // Create and execute the job
            EtlJob job = new EtlJob(config, readerFactory, writerFactory, context);
            job.execute(); // This runs the synchronous ETL process within the async thread

            // Handle successful completion
            updateJobStatus(jobId, JobStatus.COMPLETED, "Job finished successfully.");
            kibanaLogService.logJobStatus(jobId, JobStatus.COMPLETED, "Execution finished successfully", 100.0);

        } catch (Exception e) {
            // Handle execution failures
            String failureMsg = "Job execution failed: " + e.getMessage();
            log.error("Job {} failed execution.", jobId, e);
            updateJobStatus(jobId, JobStatus.FAILED, failureMsg);
            // Log final progress percentage if available, otherwise use last known value
            kibanaLogService.logJobStatus(jobId, JobStatus.FAILED, failureMsg, getProgressPercentage(jobId));
            // TODO: Trigger alerts / ITRS integration based on failure
        } finally {
            // Clean up resources associated with the job run
            jobProgressCounters.remove(jobId);
            // Optionally remove status or keep history (consider persisting status)
            // jobStatuses.remove(jobId);
            // jobStatusMessages.remove(jobId);
            MDC.remove("jobId"); // Clean up MDC for this thread
        }
        return CompletableFuture.completedFuture(null); // Indicate async task completion
    }

    private boolean canStartJob(String jobId) {
         // 1. Check if already running
         AtomicReference<JobStatus> currentStatusRef = jobStatuses.get(jobId);
         if (currentStatusRef != null && currentStatusRef.get() == JobStatus.RUNNING) {
             log.warn("Job {} is already running. Ignoring duplicate submission.", jobId);
             return false;
         }

         // 2. Basic Resource Check (can be enhanced)
         if (isResourceConstrained()) {
             log.warn("Resource constraints detected (Heap or CPU). Job {} submission delayed.", jobId);
             updateJobStatus(jobId, JobStatus.SUBMITTED, "Job submitted, waiting for resources.");
             // TODO: Implement a queuing mechanism or rely on the TaskExecutor's queue
             return false; // Prevent immediate start if constrained
         }

         // Set status to SUBMITTED before returning true to reserve the spot
         updateJobStatus(jobId, JobStatus.SUBMITTED, "Job submitted for execution.");
         return true;
    }


    @Override
    public void updateProgress(String jobId, long recordsProcessed, long totalRecords) {
        AtomicLong counter = jobProgressCounters.get(jobId);
        if (counter != null) {
            counter.set(recordsProcessed);
            // Log progress periodically based on configuration
            JobConfig config = getJobConfigFromSomewhere(jobId); // Need a way to access config if needed here
            long updateFrequency = (config != null && config.getMonitoring() != null)
                                   ? config.getMonitoring().getProgressUpdateFrequency()
                                   : 10000; // Default if config not found

            if (recordsProcessed > 0 && recordsProcessed % updateFrequency == 0) {
                 double percentage = calculatePercentage(recordsProcessed, totalRecords);
                 String message = String.format("Processing... %.2f%% (%d records)", percentage, recordsProcessed);
                 // Update status message without changing the overall RUNNING status
                 jobStatusMessages.put(jobId, message);
                 log.info("Job {} progress: {}", jobId, message);
                 kibanaLogService.logJobStatus(jobId, JobStatus.RUNNING, message, percentage); // Update Kibana
            }
        }
    }

     // --- Status Access Methods ---
     public JobStatus getJobStatus(String jobId) {
        AtomicReference<JobStatus> statusRef = jobStatuses.get(jobId);
        return (statusRef != null) ? statusRef.get() : JobStatus.UNKNOWN;
     }

      public String getJobStatusMessage(String jobId) {
         return jobStatusMessages.getOrDefault(jobId, "Status not available.");
      }

     public double getProgressPercentage(String jobId) {
         AtomicLong counter = jobProgressCounters.get(jobId);
         long processed = (counter != null) ? counter.get() : 0;
         // TODO: Need a way to get total records for the job (might be expensive)
         long total = getTotalRecordsForJob(jobId); // Placeholder
         return calculatePercentage(processed, total);
     }

     // --- Helper Methods ---

     private double calculatePercentage(long processed, long total) {
        if (total <= 0) {
            return 0.0; // Cannot calculate percentage if total is unknown or zero
        }
        if (processed >= total) {
            return 100.0;
        }
        return (double) processed / total * 100.0;
     }

     private void updateJobStatus(String jobId, JobStatus status, String message) {
         MDC.put("jobId", jobId); // Ensure context for logging status update
         log.info("Updating status for Job {}: {} - {}", jobId, status, message);
         jobStatuses.computeIfAbsent(jobId, k -> new AtomicReference<>()).set(status);
         jobStatusMessages.put(jobId, message);
         // TODO: Persist job status updates to a database for recovery and history
         MDC.remove("jobId");
     }

    private boolean isResourceConstrained() {
        try {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

            // Heap Usage Check
            long maxHeap = memoryBean.getHeapMemoryUsage().getMax();
            long usedHeap = memoryBean.getHeapMemoryUsage().getUsed();
            double heapUsagePercent = (maxHeap > 0) ? (double) usedHeap / maxHeap * 100 : 0;

            // CPU Load Check (System Load Average - might not be ideal for short checks)
            // Note: getSystemLoadAverage() might return -1 if not available.
            // Consider using a library like OSHI for more reliable CPU metrics.
            double cpuLoad = osBean.getSystemLoadAverage(); // 1-minute average
            // Simple check: if load average > number of cores * threshold %
            double cpuLoadPercent = (cpuLoad > 0 && osBean.getAvailableProcessors() > 0)
                                  ? (cpuLoad / osBean.getAvailableProcessors()) * 100
                                  : 0; // Crude approximation

            boolean heapConstrained = heapUsagePercent > maxHeapUsagePercent;
            boolean cpuConstrained = cpuLoad > 0 && cpuLoadPercent > maxCpuLoadPercent; // Only check if load is available

            if (heapConstrained) log.warn("Heap usage high: {:.2f}% (Threshold: {}%)", heapUsagePercent, maxHeapUsagePercent);
            if (cpuConstrained) log.warn("CPU load high: {:.2f}% (Threshold: {}%)", cpuLoadPercent, maxCpuLoadPercent);

            return heapConstrained || cpuConstrained;

        } catch (Exception e) {
            log.error("Error checking resource constraints: {}", e.getMessage(), e);
            return false; // Fail safe: assume not constrained if monitoring fails
        }
    }

     // Placeholder - needs implementation based on how configs are stored or passed
     private JobConfig getJobConfigFromSomewhere(String jobId) {
         // Could involve looking up in a map, database, or accessing context if available
         return null;
     }

      // Placeholder - needs implementation (e.g., query source DB or estimate)
      private long getTotalRecordsForJob(String jobId) {
          // This might involve executing a COUNT(*) query before the main read,
          // or estimating based on source file size, or might be provided in config.
          // Return -1 or 0 if unknown.
          return -1;
      }

    // TODO: Add methods for cancellation, getting detailed status, etc.
}

// --- service/KibanaLogService.java (Stub) ---
package com.yourcompany.etl.service;

import com.yourcompany.etl.core.model.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

// This service is responsible for sending structured status updates
// that can be easily parsed and visualized in Kibana.
// Option 1: Log specific JSON messages using a dedicated logger + LogstashEncoder.
// Option 2: Send data via REST to a dedicated logging endpoint or directly to Elasticsearch API.
@Service
public class KibanaLogService {

    // Use a dedicated logger category for status updates
    private static final Logger statusLog = LoggerFactory.getLogger("ETLStatusUpdates");

    public void logJobStatus(String jobId, JobStatus status, String message, double progressPercentage) {
        // Ensure jobId is in MDC for the status logger as well
        MDC.put("jobId", jobId);
        try {
            // Option 1: Log structured data (e.g., JSON via LogstashEncoder)
            // The LogstashEncoder configured in logback-spring.xml will handle JSON conversion.
            // We add specific markers or fields for easy filtering in Kibana.
            statusLog.info("StatusUpdate: Status={}, Message='{}', Progress={:.2f}",
                           status, message, progressPercentage);

            // Option 2: Direct POST to Elasticsearch/Logstash (More complex)
            // Requires HTTP client, proper indexing, security handling.
            // Example (Conceptual):
            // String payload = String.format(
            //    "{\"jobId\": \"%s\", \"timestamp\": \"%s\", \"status\": \"%s\", \"message\": \"%s\", \"progress\": %.2f}",
            //    jobId, Instant.now().toString(), status, message, progressPercentage);
            // postToElasticsearch(payload);

        } finally {
            MDC.remove("jobId");
        }
    }

    // Placeholder for Option 2
    // private void postToElasticsearch(String jsonPayload) {
    //    // Use RestTemplate, WebClient, or Elasticsearch client library
    //    log.debug("Posting to ES (stub): {}", jsonPayload);
    // }
}


// --- config/AsyncConfig.java ---
package com.yourcompany.etl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    // Define the executor bean referenced by @Async("jobExecutor")
    @Bean(name = "jobExecutor")
    public Executor jobExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // TODO: Configure core size, max size, queue capacity based on expected load and resources
        executor.setCorePoolSize(5);  // Start with 5 threads
        executor.setMaxPoolSize(10); // Allow up to 10 concurrent jobs
        executor.setQueueCapacity(25); // Queue jobs if all threads are busy
        executor.setThreadNamePrefix("ETLJobExec-");
        executor.initialize();
        return executor;
    }
}

// --- controller/EtlTriggerController.java (Stub for external triggers) ---
package com.yourcompany.etl.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourcompany.etl.model.JobConfig;
import com.yourcompany.etl.model.JobStatus;
import com.yourcompany.etl.service.WorkflowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


@RestController
@RequestMapping("/api/etl/v1")
public class EtlTriggerController {

    private static final Logger log = LoggerFactory.getLogger(EtlTriggerController.class);

    @Autowired
    private WorkflowManager workflowManager;
    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/jobs/submit")
    public ResponseEntity<Map<String, String>> submitJob(@RequestBody JobConfig jobConfig) {
        String jobId = (jobConfig != null && jobConfig.getJobId() != null) ? jobConfig.getJobId() : "UNKNOWN";
        MDC.put("jobId", jobId);
        try {
            if (jobConfig == null || jobConfig.getJobId() == null) {
                 log.error("API submission failed: Missing job configuration or jobId.");
                 return ResponseEntity.badRequest().body(Map.of("jobId", jobId, "status", "FAILED", "message", "Missing job configuration or jobId"));
            }
            log.info("Received job submission request via API for job: {}", jobId);
            // Submit but don't wait for completion here (it's async)
            workflowManager.submitJob(jobConfig);
            // Return Accepted status immediately
            return ResponseEntity.accepted().body(Map.of("jobId", jobId, "status", "SUBMITTED", "message", "Job submitted for processing."));
        } catch (Exception e) {
            log.error("API submission failed for job {}: {}", jobId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                       .body(Map.of("jobId", jobId, "status", "FAILED", "message", "Failed to submit job: " + e.getMessage()));
        } finally {
             MDC.remove("jobId");
        }
    }

    @PostMapping("/jobs/submitFromFile")
    public ResponseEntity<Map<String, String>> submitJobFromFile(@RequestParam String configPath) {
        String jobId = "fromFile-" + System.currentTimeMillis(); // Temporary ID
        MDC.put("jobId", jobId); // Use temp ID for initial logging
        try {
             Path path = Paths.get(configPath);
             if (!Files.exists(path) || !Files.isReadable(path)) {
                 log.error("Config file not found or not readable: {}", configPath);
                 return ResponseEntity.badRequest().body(Map.of("jobId", jobId, "status", "FAILED", "message", "Config file not found or not readable: " + configPath));
             }

             log.info("Reading job configuration from file: {}", configPath);
             String configJson = new String(Files.readAllBytes(path));
             JobConfig jobConfig = objectMapper.readValue(configJson, JobConfig.class);

             // Use actual jobId from config file for subsequent operations and logging
             jobId = jobConfig.getJobId();
             MDC.put("jobId", jobId); // Update MDC with actual jobId
             log.info("Submitting job from file {} with Job ID: {}", configPath, jobId);

             workflowManager.submitJob(jobConfig);
             return ResponseEntity.accepted().body(Map.of("jobId", jobId, "status", "SUBMITTED", "message", "Job submitted from file: " + configPath));
         } catch (Exception e) {
            log.error("API submission from file {} failed for job {}: {}", configPath, jobId, e.getMessage(), e);
            // Ensure MDC has the best known jobId for error logging
            MDC.put("jobId", jobId);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                       .body(Map.of("jobId", jobId, "status", "FAILED", "message", "Failed to submit job from file " + configPath + ": " + e.getMessage()));
        } finally {
             MDC.remove("jobId");
        }
    }

     @GetMapping("/jobs/{jobId}/status")
     public ResponseEntity<Map<String, Object>> getJobStatus(@PathVariable String jobId) {
         MDC.put("jobId", jobId);
         try {
             JobStatus status = workflowManager.getJobStatus(jobId);
             String message = workflowManager.getJobStatusMessage(jobId);
             double progress = workflowManager.getProgressPercentage(jobId);

             Map<String, Object> response = new HashMap<>();
             response.put("jobId", jobId);
             response.put("status", status);
             response.put("message", message);
             response.put("progressPercent", String.format("%.2f", progress)); // Format progress

             if (status == JobStatus.UNKNOWN) {
                 return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
             }
             return ResponseEntity.ok(response);
         } finally {
             MDC.remove("jobId");
         }
     }

     // TODO: Add endpoint for job cancellation if needed
     // @PostMapping("/jobs/{jobId}/cancel")
     // public ResponseEntity<Map<String, String>> cancelJob(@PathVariable String jobId) { ... }
}

// --- Add application.yml configuration ---
/*
server:
  port: 8080 # Or your preferred port

spring:
  application:
    name: etl-workflow-engine
  kafka:
    bootstrap-servers: your_kafka_brokers:9092 # Replace with your Kafka brokers
    consumer:
      group-id: etl-workflow-consumers # Consumer group ID
      auto-offset-reset: earliest # Or latest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
         # Add any other required Kafka consumer properties
         # isolation.level: read_committed # If using Kafka transactions

etl:
  kafka:
    topic:
      workflow: etl-workflow-queue # Kafka topic to consume from
  engine:
    monitoring:
      max-heap-usage-percent: 85 # Threshold for heap usage before delaying jobs
      max-cpu-load-percent: 90   # Threshold for CPU load before delaying jobs
    # Add DataSource configurations here if WorkflowManager needs DB access
    # for status persistence

logging:
  level:
    com.yourcompany.etl: INFO # Set default level for your ETL code
    ETLJob: DEBUG # Set finer-grained logging for individual jobs if needed
    ETLStatusUpdates: INFO # Ensure status updates are logged
  # Reference logback configuration file
  config: classpath:logback-spring.xml

management:
  endpoints:
    web:
      exposure:
        include: "health,info,metrics,prometheus,loggers" # Expose actuator endpoints
  endpoint:
    health:
      show-details: when_authorized
  metrics:
    tags:
      application: ${spring.application.name}

*/
