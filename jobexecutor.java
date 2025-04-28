package com.yourcompany.etl.core;

import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.exception.JobCancelledException;
import com.yourcompany.etl.core.factory.DataReaderFactory;
import com.yourcompany.etl.core.factory.DataWriterFactory;
// TODO: Add TransformerFactory and Transformer interface if/when needed for Stage 2
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Row;
import com.yourcompany.etl.core.processor.MappingProcessor;
import com.yourcompany.etl.core.reader.DataReader;
import com.yourcompany.etl.core.writer.DataWriter;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Represents a single ETL job execution.
 * Orchestrates the reading, processing (mapping/casting), transforming (optional),
 * and writing of data.
 */
public class EtlJob {

    private final JobConfig config;
    private final DataReaderFactory readerFactory;
    private final DataWriterFactory writerFactory;
    // private final TransformerFactory transformerFactory; // For Stage 2
    private final JobContext context;
    private final Logger log;
    private final JobConfig.ErrorHandling errorHandlingConfig;

    // TODO: Inject TransformerFactory when needed
    public EtlJob(JobConfig config, DataReaderFactory readerFactory, DataWriterFactory writerFactory, JobContext context) {
        this.config = config;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        // this.transformerFactory = transformerFactory;
        this.context = context;
        this.log = context.getLogger(); // Use job-specific logger from context
        this.errorHandlingConfig = config.getErrorHandling(); // Cache for quick access
    }

    /**
     * Executes the ETL job flow: open -> read/process/write -> close.
     * Manages resources using try-with-resources.
     *
     * @throws Exception If a non-recoverable error occurs during execution.
     */
    public void execute() throws Exception {
        log.info("Starting ETL job execution...");
        long startTime = System.nanoTime();
        long recordsProcessedSuccessfully = 0; // Counter for successfully written records

        // Use try-with-resources to ensure readers/writers are closed
        try (DataReader reader = readerFactory.createReader(config.getSource().getType());
             DataWriter writer = writerFactory.createWriter(config.getDestination().getType())
             // TODO: Add ErrorWriter if routing errors (e.g., to a file or topic)
             // ErrorWriter errorWriter = errorWriterFactory.createWriter(...)
             )
        {
            log.info("Opening reader and writer...");
            reader.open(context);
            writer.open(context);
            // errorWriter.open(context);
            log.info("Reader and writer opened successfully.");

            // Initialize processor and potentially transformer
            MappingProcessor processor = new MappingProcessor(context);
            // Transformer transformer = createTransformerIfNeeded(); // Stage 2

            // Get batch size from config, provide a sensible default
            int batchSize = config.getDestination().getBatchSize() > 0 ? config.getDestination().getBatchSize() : 1000;
            List<Row> batch = new ArrayList<>(batchSize);

            // Get total records if available for progress reporting
            try {
                 long total = reader.getTotalRecords();
                 context.setTotalSourceRecords(total);
                 log.info("Total source records estimated: {}", (total <= 0 ? "Unknown" : total));
            } catch (Exception e) {
                 log.warn("Could not determine total source records: {}", e.getMessage());
                 context.setTotalSourceRecords(-1);
            }


            log.info("Starting data streaming...");
            try (Stream<Row> stream = reader.stream()) {

                for (Row sourceRow : (Iterable<Row>) stream::iterator) {
                    context.checkIfCancelled(); // Check for cancellation request

                    Row processedRow = null;
                    try {
                        // 1. Process (Map fields, Cast types)
                        processedRow = processor.process(sourceRow);

                        // If processor returns null, it means the row had a handled error (logged/routed)
                        if (processedRow == null) {
                            log.debug("Skipping row due to handled processing error. Record count (read): {}", context.getRecordsRead());
                            continue; // Skip to the next source row
                        }

                        // 2. Transform (Optional - Stage 2)
                        // Row transformedRow = (transformer != null) ? transformer.transform(processedRow) : processedRow;
                        Row finalRow = processedRow; // Replace with transformedRow later

                        // 3. Add to batch for writing
                        batch.add(finalRow);

                        // 4. Write batch when full
                        if (batch.size() >= batchSize) {
                            log.debug("Batch full ({} rows), writing batch...", batch.size());
                            writeBatchWithRetry(writer, batch); // Use retry logic for writes
                            recordsProcessedSuccessfully += batch.size();
                            context.incrementRecordsWritten(batch.size()); // Update context after successful write
                            batch.clear();
                            log.debug("Batch written successfully. Total written: {}", context.getRecordsWritten());
                        }

                    } catch (JobCancelledException e) {
                         log.warn("Job execution cancelled.");
                         throw e; // Re-throw cancellation exception to stop processing
                    } catch (EtlProcessingException e) {
                        // This catch block handles errors thrown by the processor when FAIL_JOB is set,
                        // or if maxErrors is exceeded.
                        log.error("Stopping job due to unrecoverable processing error: {}", e.getMessage(), e);
                        // Write any partially filled batch before failing? Maybe not, data might be inconsistent.
                        batch.clear(); // Discard current batch on failure
                        throw e; // Re-throw to signal job failure
                    } catch (Exception e) {
                        // Catch unexpected errors during the loop (e.g., stream issues)
                        log.error("Unexpected error during processing loop: {}", e.getMessage(), e);
                        batch.clear(); // Discard current batch
                        throw new EtlProcessingException("Unexpected error during processing loop", e); // Wrap and re-throw
                    }
                } // End of stream iteration

                // Write any remaining records in the last batch
                if (!batch.isEmpty()) {
                     log.debug("Writing final batch of {} rows...", batch.size());
                     writeBatchWithRetry(writer, batch);
                     recordsProcessedSuccessfully += batch.size();
                     context.incrementRecordsWritten(batch.size()); // Update context
                     batch.clear();
                     log.debug("Final batch written successfully. Total written: {}", context.getRecordsWritten());
                }

            } // Stream is automatically closed here

            long endTime = System.nanoTime();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            log.info("ETL Job completed successfully.");
            log.info("Summary: Records Read: {}, Records Written: {}, Records Failed: {}, Duration: {} ms",
                     context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed(), durationMillis);

        } catch (JobCancelledException e) {
             log.warn("Job {} execution was cancelled.", context.getJobId());
             // Don't re-throw as a generic Exception, let the WorkflowManager handle specific status
             throw e; // Allow WorkflowManager to catch and set CANCELLED status
        } catch (Exception e) {
            long endTime = System.nanoTime();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            log.error("ETL Job failed after {} ms: {}", durationMillis, e.getMessage(), e);
            log.error("Failure Summary: Records Read: {}, Records Written: {}, Records Failed: {}",
                      context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed());
            // Ensure resources are closed by try-with-resources, then re-throw
            throw e;
        }
        // Implicitly calls close() on reader and writer due to try-with-resources
        log.info("Reader and writer closed automatically.");
    }

    /**
     * Writes a batch using the DataWriter, implementing a simple retry mechanism
     * for potentially transient errors (e.g., network issues, temporary DB locks).
     *
     * @param writer The DataWriter instance.
     * @param batch  The list of rows to write.
     * @throws Exception If writing fails after all retry attempts.
     */
     private void writeBatchWithRetry(DataWriter writer, List<Row> batch) throws Exception {
         // TODO: Make retry count and delay configurable via JobConfig
         int maxRetries = 3;
         long delayMs = 1000; // Initial delay
         int attempt = 0;

         while (true) {
             try {
                 context.checkIfCancelled(); // Check cancellation before write attempt
                 writer.writeBatch(batch);
                 return; // Success
             } catch (JobCancelledException e) {
                  log.warn("Write cancelled during retry attempt.");
                  throw e; // Propagate cancellation immediately
             } catch (Exception e) {
                 attempt++;
                 log.warn("Write batch attempt {}/{} failed for job {}: {}", attempt, maxRetries, context.getJobId(), e.getMessage());

                 if (attempt >= maxRetries || !isRetryable(e)) { // Check if error is retryable
                     log.error("Write batch failed permanently after {} attempts for job {}.", attempt, context.getJobId(), e);
                     // TODO: Handle persistent write failure (e.g., route failed batch to error location)
                     // errorWriter.writeFailedBatch(batch, e);
                     throw new EtlProcessingException("Write batch failed after " + attempt + " attempts", e); // Wrap in ETL exception
                 }

                 // Wait before retrying (consider exponential backoff)
                 try {
                     long currentDelay = delayMs * (long)Math.pow(2, attempt - 1); // Exponential backoff
                     log.info("Waiting {} ms before retry attempt {}...", currentDelay, attempt + 1);
                     Thread.sleep(currentDelay);
                 } catch (InterruptedException ie) {
                     Thread.currentThread().interrupt(); // Restore interrupt status
                     log.warn("Retry delay interrupted for job {}.", context.getJobId());
                     throw new JobCancelledException("Job " + context.getJobId() + " interrupted during write retry delay.");
                 }
             }
         }
     }

     /**
      * Determines if an exception caught during writing is likely transient and worth retrying.
      * This needs to be adapted based on specific database/destination error codes or exception types.
      * @param e The exception caught.
      * @return true if the error might be transient, false otherwise.
      */
     private boolean isRetryable(Exception e) {
         // Basic example: Retry IOExceptions, SQLTransientExceptions
         if (e instanceof java.io.IOException) {
             return true;
         }
         if (e instanceof java.sql.SQLTransientException) {
             return true;
         }
         // Add specific SQLState checks or other destination-specific transient error types
         // e.g., if (e instanceof SQLException && ((SQLException)e).getSQLState().startsWith("some_transient_code")) return true;

         // If wrapped, check the cause
         Throwable cause = e.getCause();
         if (cause != null && cause instanceof Exception) {
              if (cause instanceof java.io.IOException) return true;
              if (cause instanceof java.sql.SQLTransientException) return true;
         }

         log.warn("Exception type {} deemed non-retryable.", e.getClass().getName());
         return false; // Default to non-retryable for unknown errors
     }

    // TODO: Implement createTransformerIfNeeded() for Stage 2
    // private Transformer createTransformerIfNeeded() {
    //    if (config.getTransformation() != null && !"NONE".equalsIgnoreCase(config.getTransformation().getType())) {
    //        return transformerFactory.createTransformer(config.getTransformation().getType());
    //    }
    //    return null; // No transformation needed
    // }
}
