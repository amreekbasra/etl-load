// --- core/model/Row.java ---
package com.yourcompany.etl.core.model;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single row of data being processed.
 * Uses a Map to store data dynamically, keyed by destination field names after processing.
 * Before processing (in the reader), keys might be source field names.
 */
public class Row {
    private final Map<String, Object> data;

    public Row(Map<String, Object> data) {
        // Ensure the map is mutable if needed downstream, or wrap if immutability is desired
        this.data = Objects.requireNonNull(data, "Row data cannot be null");
    }

    public Map<String, Object> getData() {
        // Return an unmodifiable view if the Row should be immutable after creation
        // return Collections.unmodifiableMap(data);
        return data;
    }

    public Object getValue(String fieldName) {
        return data.get(fieldName);
    }

    // Optional: Add helper methods for typed access, e.g., getString, getLong
    public String getString(String fieldName) {
        Object value = data.get(fieldName);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public String toString() {
        return "Row{" + "data=" + data + '}';
    }
}

// --- core/model/JobConfig.java ---
// This should ideally live in the core module if shared across engine and potentially other tools.
// See the definition provided in the etl-workflow-engine-v1 immersive document.
// Ensure all necessary classes (SourceConfig, DestinationConfig, Mapping, etc.) are defined here.
package com.yourcompany.etl.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {
    private String jobId;
    private int priority;
    private SourceConfig source;
    private DestinationConfig destination;
    private List<Mapping> mappings;
    private ErrorHandling errorHandling;
    private Transformation transformation;
    private Monitoring monitoring;

    @Data @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SourceConfig {
        private String type;
        private Map<String, Object> connectionDetails;
        // Add fetchSize directly if common, or keep in map
        // private int fetchSize = 1000;
    }

     @Data @JsonIgnoreProperties(ignoreUnknown = true)
     public static class DestinationConfig {
         private String type;
         private Map<String, Object> connectionDetails;
         private int batchSize = 1000;
     }

     @Data @JsonIgnoreProperties(ignoreUnknown = true)
     public static class Mapping {
        private String sourceFieldName;
        private String destinationFieldName;
        private String sourceFieldType; // e.g., "VARCHAR2", "NUMBER", "DATE" (as string from source)
        private String destFieldType;   // e.g., "STRING", "LONG", "TIMESTAMP" (target Java/destination type)
        private boolean isSourceNullable = true; // Default to true if not specified
        private boolean isDestNullable = true;   // Default to true if not specified
     }

     @Data @JsonIgnoreProperties(ignoreUnknown = true)
     public static class ErrorHandling {
         private String strategy = "FAIL_JOB";
         private String errorFilePath;
         private long maxErrorsAllowed = 0;
     }

     @Data @JsonIgnoreProperties(ignoreUnknown = true)
     public static class Transformation {
         private String type = "NONE";
         private String scriptPath;
         private Map<String, Object> parameters;
     }

      @Data @JsonIgnoreProperties(ignoreUnknown = true)
      public static class Monitoring {
          private long progressUpdateFrequency = 10000;
      }
}


// --- core/model/JobStatus.java ---
package com.yourcompany.etl.core.model;

public enum JobStatus {
    UNKNOWN,
    SUBMITTED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}

// --- core/JobProgressListener.java ---
package com.yourcompany.etl.core;

/**
 * Callback interface for reporting job progress.
 * Implemented by the WorkflowManager or another monitoring component.
 */
public interface JobProgressListener {
    /**
     * Updates the progress for a given job.
     * @param jobId The unique identifier of the job.
     * @param recordsProcessed The number of records successfully processed so far.
     * @param totalRecords The total number of records expected (if known, otherwise <= 0).
     */
    void updateProgress(String jobId, long recordsProcessed, long totalRecords);
}


// --- core/JobContext.java ---
package com.yourcompany.etl.core;

import com.yourcompany.etl.core.model.JobConfig;
import org.slf4j.Logger; // Use SLF4j for logging facade
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds contextual information for a single ETL job execution run.
 * Passed to readers, writers, processors.
 */
public class JobContext {
    private final String jobId;
    private final JobProgressListener progressListener;
    private final JobConfig config;
    private final Logger logger; // Job-specific logger instance
    private final AtomicLong recordsRead = new AtomicLong(0);
    private final AtomicLong recordsWritten = new AtomicLong(0);
    private final AtomicLong recordsFailed = new AtomicLong(0);
    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    private long totalSourceRecords = -1; // Cache total records if calculated

    public JobContext(String jobId, JobProgressListener progressListener, JobConfig config, Logger logger) {
        this.jobId = jobId;
        this.progressListener = progressListener;
        this.config = config;
        this.logger = logger;
    }

    public String getJobId() {
        return jobId;
    }

    public JobProgressListener getProgressListener() {
        return progressListener;
    }

    public JobConfig getConfig() {
        return config;
    }

    public Logger getLogger() {
        return logger;
    }

    // --- Progress Tracking Methods ---
    public long incrementAndGetRecordsRead() {
        return recordsRead.incrementAndGet();
    }

    public long getRecordsRead() {
        return recordsRead.get();
    }

    public void incrementRecordsWritten(long count) {
        recordsWritten.addAndGet(count);
        // Report progress based on records written (more accurate for final output)
        progressListener.updateProgress(jobId, recordsWritten.get(), totalSourceRecords);
    }

     public long getRecordsWritten() {
        return recordsWritten.get();
    }

    public long incrementAndGetRecordsFailed() {
        return recordsFailed.incrementAndGet();
    }

     public long getRecordsFailed() {
        return recordsFailed.get();
    }

    public void setTotalSourceRecords(long total) {
        this.totalSourceRecords = total;
    }

     public long getTotalSourceRecords() {
        return totalSourceRecords;
    }

    // --- Cancellation Handling ---
    public void requestCancellation() {
        this.cancellationRequested.set(true);
        logger.warn("Cancellation requested for job {}", jobId);
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    /**
     * Checks if cancellation has been requested and throws an exception if it has.
     * Should be called periodically within long-running read/write loops.
     * @throws JobCancelledException if cancellation has been requested.
     */
    public void checkIfCancelled() throws JobCancelledException {
        if (isCancellationRequested()) {
            throw new JobCancelledException("Job " + jobId + " was cancelled.");
        }
    }
}

// --- core/exception/JobCancelledException.java ---
package com.yourcompany.etl.core.exception;

public class JobCancelledException extends RuntimeException {
    public JobCancelledException(String message) {
        super(message);
    }
}

// --- core/exception/EtlProcessingException.java ---
package com.yourcompany.etl.core.exception;

import com.yourcompany.etl.core.model.Row;

/**
 * Custom exception for errors during row processing (mapping, transformation, validation).
 */
public class EtlProcessingException extends RuntimeException {
    private final Row errorRow; // Optionally include the row that caused the error

    public EtlProcessingException(String message, Throwable cause, Row errorRow) {
        super(message, cause);
        this.errorRow = errorRow;
    }

     public EtlProcessingException(String message, Row errorRow) {
        super(message);
        this.errorRow = errorRow;
    }

    public EtlProcessingException(String message, Throwable cause) {
        super(message, cause);
        this.errorRow = null;
    }

     public EtlProcessingException(String message) {
        super(message);
        this.errorRow = null;
    }

    public Row getErrorRow() {
        return errorRow;
    }
}


// --- core/reader/DataReader.java ---
package com.yourcompany.etl.core.reader;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.model.Row;
import java.io.Closeable; // Use Closeable for try-with-resources
import java.util.stream.Stream;

/**
 * Interface for reading data from a source system.
 * Implementations must handle resource management (opening/closing connections).
 */
public interface DataReader extends Closeable { // AutoCloseable is also fine

    /**
     * Opens the reader and initializes resources (e.g., DB connection, file handle).
     * @param context The context for the current job run.
     * @throws Exception If initialization fails.
     */
    void open(JobContext context) throws Exception;

    /**
     * Provides a Stream of Rows from the source.
     * The implementation must ensure that data is streamed and not loaded entirely into memory.
     * The stream should handle resource cleanup when closed (e.g., via onClose).
     * @return A Stream of Row objects.
     * @throws Exception If streaming fails.
     */
    Stream<Row> stream() throws Exception;

    /**
     * Optionally, returns the total number of records expected from the source.
     * This might involve a separate query (e.g., COUNT(*)) and can be expensive.
     * Return -1 or 0 if the total count is unknown or too costly to determine.
     * @return Total expected records, or a non-positive value if unknown.
     * @throws Exception If counting fails.
     */
    long getTotalRecords() throws Exception;

    /**
     * Closes the reader and releases all resources.
     * This method is automatically called when using try-with-resources.
     * @throws Exception If closing fails.
     */
    @Override
    void close() throws Exception;
}


// --- core/writer/DataWriter.java ---
package com.yourcompany.etl.core.writer;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.model.Row;
import java.io.Closeable;
import java.util.List;

/**
 * Interface for writing data to a destination system.
 * Implementations handle resource management and batching.
 */
public interface DataWriter extends Closeable {

    /**
     * Opens the writer and initializes resources (e.g., DB connection, file handle).
     * @param context The context for the current job run.
     * @throws Exception If initialization fails.
     */
    void open(JobContext context) throws Exception;

    /**
     * Writes a batch of rows to the destination.
     * Implementations should handle batching logic efficiently.
     * Implementations must also handle retries internally if appropriate for transient errors.
     * @param batch A list of Row objects to write.
     * @throws Exception If writing fails after retries (or immediately for non-recoverable errors).
     */
    void writeBatch(List<Row> batch) throws Exception;

    /**
     * Closes the writer, ensuring all buffered data is flushed and resources are released.
     * Automatically called by try-with-resources.
     * @throws Exception If closing or flushing fails.
     */
    @Override
    void close() throws Exception;
}


// --- core/factory/DataReaderFactory.java ---
package com.yourcompany.etl.core.factory;

import com.yourcompany.etl.core.reader.DataReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Factory responsible for creating DataReader instances based on source type.
 * Uses Spring's ApplicationContext to get specific reader beans.
 */
@Component
public class DataReaderFactory {

    @Autowired
    private ApplicationContext context;

    /**
     * Creates a DataReader instance.
     * @param sourceType The type identifier (e.g., "ORACLE_DB", "FLAT_FILE"). Should match bean names or a mapping logic.
     * @return A new DataReader instance.
     * @throws IllegalArgumentException if no reader is found for the type.
     */
    public DataReader createReader(String sourceType) {
        try {
            // Assumes bean names match the sourceType constants (e.g., "oracleDbReader", "flatFileReader")
            // Adjust the bean naming convention as needed.
            String beanName = sourceType.toLowerCase().replace("_", "") + "Reader"; // e.g., oracledbReader, flatfileReader
             // Alternatively, use a Map<String, Class<? extends DataReader>> or Map<String, Provider<DataReader>>
            return context.getBean(beanName, DataReader.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("No DataReader bean configured for source type: " + sourceType, e);
        }
    }
}

// --- core/factory/DataWriterFactory.java ---
package com.yourcompany.etl.core.factory;

import com.yourcompany.etl.core.writer.DataWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Factory responsible for creating DataWriter instances based on destination type.
 */
@Component
public class DataWriterFactory {

    @Autowired
    private ApplicationContext context;

    /**
     * Creates a DataWriter instance.
     * @param destinationType The type identifier (e.g., "ORACLE_DB", "FLAT_FILE", "ELASTICSEARCH").
     * @return A new DataWriter instance.
     * @throws IllegalArgumentException if no writer is found for the type.
     */
    public DataWriter createWriter(String destinationType) {
         try {
            String beanName = destinationType.toLowerCase().replace("_", "") + "Writer"; // e.g., oracledbWriter, flatfileWriter
            return context.getBean(beanName, DataWriter.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("No DataWriter bean configured for destination type: " + destinationType, e);
        }
    }
}
