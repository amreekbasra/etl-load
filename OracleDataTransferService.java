package com.yourcompany.etl.service;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.exception.JobCancelledException;
import com.yourcompany.etl.core.factory.DataReaderFactory;
import com.yourcompany.etl.core.factory.DataWriterFactory;
// TODO: Add ErrorWriterFactory if implementing error routing
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Row;
import com.yourcompany.etl.core.processor.MappingProcessor;
import com.yourcompany.etl.core.reader.DataReader;
import com.yourcompany.etl.core.writer.DataWriter;
// TODO: Add ErrorWriter interface/impl
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Service responsible for orchestrating data transfer specifically between
 * Oracle source and Oracle destination using the DataReader/DataWriter pattern.
 * This is similar to the generic EtlJob class but could be specialized.
 */
@Service
public class OracleDataTransferService {

    private static final Logger log = LoggerFactory.getLogger(OracleDataTransferService.class); // Generic logger for the service itself

    @Autowired
    private DataReaderFactory readerFactory; // Factory to get specific reader instances

    @Autowired
    private DataWriterFactory writerFactory; // Factory to get specific writer instances

    // TODO: @Autowired private ErrorWriterFactory errorWriterFactory;

    /**
     * Executes the data transfer process defined by the JobConfig.
     * Assumes the JobConfig specifies Oracle source and destination types.
     *
     * @param context The JobContext containing configuration, logger, and progress listener.
     * @throws Exception If a non-recoverable error occurs during the transfer.
     */
    public void transferData(JobContext context) throws Exception {
        JobConfig config = context.getConfig();
        Logger jobLogger = context.getLogger(); // Use the job-specific logger from context

        jobLogger.info("Starting Oracle-to-Oracle data transfer for Job ID: {}", context.getJobId());
        long startTime = System.nanoTime();
        long recordsProcessedSuccessfully = 0; // Counter for successfully written records

        // Validate source/destination types if needed
        if (!"ORACLE_DB".equalsIgnoreCase(config.getSource().getType()) || !"ORACLE_DB".equalsIgnoreCase(config.getDestination().getType())) {
             throw new EtlProcessingException("OracleDataTransferService requires source and destination types to be 'ORACLE_DB'. Found: Source="
                                               + config.getSource().getType() + ", Dest=" + config.getDestination().getType());
        }


        // Use try-with-resources for automatic resource management
        try (DataReader reader = readerFactory.createReader(config.getSource().getType()); // Should resolve to OracleJdbcReader
             DataWriter writer = writerFactory.createWriter(config.getDestination().getType()) // Should resolve to OracleJdbcWriter
             // TODO: ErrorWriter errorWriter = createErrorWriterIfNeeded(context);
             )
        {
            jobLogger.info("Opening Oracle reader and writer...");
            reader.open(context);
            writer.open(context);
            // errorWriter.open(context);
            jobLogger.info("Oracle reader and writer opened successfully.");

            MappingProcessor processor = new MappingProcessor(context);
            // Transformer logic would go here if needed

            int batchSize = config.getDestination().getBatchSize() > 0 ? config.getDestination().getBatchSize() : 1000;
            List<Row> batch = new ArrayList<>(batchSize);

            // Estimate total records for progress reporting
            try {
                 long total = reader.getTotalRecords();
                 context.setTotalSourceRecords(total);
                 jobLogger.info("Total source records estimated: {}", (total <= 0 ? "Unknown" : total));
            } catch (Exception e) {
                 jobLogger.warn("Could not determine total source records: {}", e.getMessage());
                 context.setTotalSourceRecords(-1);
            }

            jobLogger.info("Starting data streaming and processing...");
            try (Stream<Row> stream = reader.stream()) {

                for (Row sourceRow : (Iterable<Row>) stream::iterator) {
                    context.checkIfCancelled(); // Check frequently

                    Row processedRow = null;
                    try {
                        // 1. Process (Map fields, Cast types)
                        processedRow = processor.process(sourceRow);

                        if (processedRow == null) {
                            // Row had a handled error (logged/routed by processor)
                            jobLogger.debug("Skipping row due to handled processing error. Read count: {}", context.getRecordsRead());
                            continue; // Skip to next source row
                        }

                        // 2. Transform (if applicable)
                        Row finalRow = processedRow; // Assign transformedRow if transformation exists

                        // 3. Add to batch
                        batch.add(finalRow);

                        // 4. Write batch when full
                        if (batch.size() >= batchSize) {
                            jobLogger.debug("Batch full ({} rows), writing batch...", batch.size());
                            writeBatchWithRetry(writer, batch, context); // Pass context for logging/cancellation check
                            recordsProcessedSuccessfully += batch.size();
                            context.incrementRecordsWritten(batch.size());
                            batch.clear();
                            jobLogger.debug("Batch written. Total written: {}", context.getRecordsWritten());
                        }

                    } catch (JobCancelledException e) {
                         jobLogger.warn("Job execution cancelled during stream processing.");
                         throw e;
                    } catch (EtlProcessingException e) {
                        // Handles FAIL_JOB strategy from processor or maxErrors exceeded
                        jobLogger.error("Stopping job due to unrecoverable processing error: {}", e.getMessage(), e);
                        batch.clear(); // Discard current batch
                        throw e;
                    } catch (Exception e) {
                        // Catch unexpected errors during the loop
                        jobLogger.error("Unexpected error during processing loop: {}", e.getMessage(), e);
                        batch.clear();
                        throw new EtlProcessingException("Unexpected error during processing loop", e);
                    }
                } // End of stream iteration

                // Write the final batch
                if (!batch.isEmpty()) {
                     jobLogger.debug("Writing final batch of {} rows...", batch.size());
                     writeBatchWithRetry(writer, batch, context);
                     recordsProcessedSuccessfully += batch.size();
                     context.incrementRecordsWritten(batch.size());
                     batch.clear();
                     jobLogger.debug("Final batch written. Total written: {}", context.getRecordsWritten());
                }

            } // Stream automatically closed

            long endTime = System.nanoTime();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            jobLogger.info("Oracle data transfer completed successfully.");
            jobLogger.info("Transfer Summary: Records Read: {}, Records Written: {}, Records Failed: {}, Duration: {} ms",
                           context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed(), durationMillis);

        } catch (JobCancelledException e) {
             jobLogger.warn("Job {} execution was cancelled.", context.getJobId());
             throw e; // Let caller (WorkflowManager) handle status update
        } catch (Exception e) {
            long endTime = System.nanoTime();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            jobLogger.error("Oracle data transfer failed after {} ms: {}", durationMillis, e.getMessage(), e);
            jobLogger.error("Failure Summary: Records Read: {}, Records Written: {}, Records Failed: {}",
                            context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed());
            throw e; // Propagate exception for failure handling
        }
        // Resources closed by try-with-resources
        jobLogger.info("Oracle reader and writer closed automatically.");
    }


     /**
      * Wraps the writer's writeBatch call with retry logic.
      */
     private void writeBatchWithRetry(DataWriter writer, List<Row> batch, JobContext context) throws Exception {
         // TODO: Make retry count and delay configurable
         int maxRetries = 3;
         long delayMs = 1000;
         int attempt = 0;
         Logger jobLogger = context.getLogger();

         while (true) {
             try {
                 context.checkIfCancelled(); // Check before each attempt
                 writer.writeBatch(batch); // Delegate to the writer's implementation
                 return; // Success
             } catch (JobCancelledException e) {
                  jobLogger.warn("Write cancelled during retry attempt.");
                  throw e;
             } catch (Exception e) {
                 attempt++;
                 jobLogger.warn("Write batch attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                 if (attempt >= maxRetries || !isRetryable(e)) {
                     jobLogger.error("Write batch failed permanently after {} attempts.", attempt, e);
                     // TODO: Implement routing of failed batch if configured
                     // errorWriter.writeFailedBatch(batch, e);
                     throw new EtlProcessingException("Write batch failed after " + attempt + " attempts", e);
                 }

                 try {
                     long currentDelay = delayMs * (long)Math.pow(2, attempt - 1);
                     jobLogger.info("Waiting {} ms before write retry attempt {}...", currentDelay, attempt + 1);
                     Thread.sleep(currentDelay);
                 } catch (InterruptedException ie) {
                     Thread.currentThread().interrupt();
                     jobLogger.warn("Retry delay interrupted.");
                     throw new JobCancelledException("Job " + context.getJobId() + " interrupted during write retry delay.");
                 }
             }
         }
     }

     /**
      * Checks if an exception is likely transient and suitable for retry.
      * (Same logic as in EtlJob example, kept here for completeness of the service)
      */
     private boolean isRetryable(Exception e) {
         if (e instanceof java.io.IOException) return true;
         if (e instanceof java.sql.SQLTransientException) return true;
         // Add Oracle specific transient error codes (e.g., ORA-xxxxx for timeouts, lock waits)
         // if (e instanceof java.sql.SQLException) {
         //    int errorCode = ((java.sql.SQLException) e).getErrorCode();
         //    if (errorCode == ORA_TIMEOUT_CODE || errorCode == ORA_LOCK_WAIT_CODE) return true;
         // }

         Throwable cause = e.getCause();
         if (cause != null && cause instanceof Exception) {
              if (cause instanceof java.io.IOException) return true;
              if (cause instanceof java.sql.SQLTransientException) return true;
              // Check cause for Oracle specific codes too
         }
         return false;
     }

     // TODO: Implement error writer creation if needed
     // private ErrorWriter createErrorWriterIfNeeded(JobContext context) throws Exception { ... }

}
