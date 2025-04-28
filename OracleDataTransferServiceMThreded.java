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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.TaskExecutor; // Use Spring's TaskExecutor abstraction
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Service responsible for orchestrating data transfer specifically between
 * Oracle source and Oracle destination, implementing parallel writing.
 */
@Service
public class OracleDataTransferService {

    private static final Logger log = LoggerFactory.getLogger(OracleDataTransferService.class);

    @Autowired
    private DataReaderFactory readerFactory;

    @Autowired
    private DataWriterFactory writerFactory;

    // Inject the application context to get prototype-scoped beans (writers)
    @Autowired
    private ApplicationContext applicationContext;

    // Inject a TaskExecutor specifically configured for parallel writers within a job
    // This could be the same as jobExecutor or a different one.
    @Autowired
    @Qualifier("writerTaskExecutor") // Use a specific executor for writers
    private TaskExecutor writerTaskExecutor;

    // Configuration for parallelism and queue size (from application.yml)
    @Value("${etl.engine.writer.parallelism:4}") // Default to 4 parallel writers
    private int parallelismLevel;

    @Value("${etl.engine.writer.queueCapacity:10000}") // Max rows buffered between reader/writers
    private int queueCapacity;

    // Special marker object to signal the end of data in the queue
    private static final Row END_OF_DATA_MARKER = new Row(null); // Use a unique marker instance

    /**
     * Executes the data transfer process defined by the JobConfig using parallel writers.
     *
     * @param context The JobContext containing configuration, logger, and progress listener.
     * @throws Exception If a non-recoverable error occurs during the transfer.
     */
    public void transferData(JobContext context) throws Exception {
        JobConfig config = context.getConfig();
        Logger jobLogger = context.getLogger();

        jobLogger.info("Starting PARALLEL Oracle-to-Oracle data transfer for Job ID: {}. Parallelism: {}, Queue Capacity: {}",
                     context.getJobId(), parallelismLevel, queueCapacity);
        long startTime = System.nanoTime();

        // Validate source/destination types
        validateConfig(config);

        // Shared queue between reader and writers
        BlockingQueue<Row> queue = new ArrayBlockingQueue<>(queueCapacity);

        // Flag to signal errors from any task
        AtomicBoolean errorOccurred = new AtomicBoolean(false);
        // Latch to wait for all writer tasks to complete
        CountDownLatch writerTasksLatch = new CountDownLatch(parallelismLevel);

        List<Future<?>> writerFutures = new ArrayList<>();
        ExecutorService readerExecutor = Executors.newSingleThreadExecutor(); // Dedicated thread for reader
        Future<?> readerFuture = null;

        // Use try-with-resources for the main reader only
        try (DataReader reader = readerFactory.createReader(config.getSource().getType())) {
            jobLogger.info("Opening Oracle reader...");
            reader.open(context);
            jobLogger.info("Oracle reader opened successfully.");

            // Estimate total records
            estimateTotalRecords(reader, context);

            // --- Start Reader Task ---
            readerFuture = readerExecutor.submit(() -> {
                jobLogger.info("Reader task started.");
                try (Stream<Row> stream = reader.stream()) {
                    for (Row sourceRow : (Iterable<Row>) stream::iterator) {
                        if (errorOccurred.get() || context.isCancellationRequested()) {
                            jobLogger.warn("Reader task stopping due to error or cancellation request.");
                            break; // Stop reading if an error occurred elsewhere or cancelled
                        }
                        // Put row onto the queue, blocking if full
                        queue.put(sourceRow); // Throws InterruptedException
                    }
                    jobLogger.info("Reader task finished reading data.");
                } catch (InterruptedException e) {
                    jobLogger.warn("Reader task interrupted while putting data on queue.");
                    Thread.currentThread().interrupt();
                    errorOccurred.set(true);
                } catch (Exception e) {
                    jobLogger.error("Error during reader task execution: {}", e.getMessage(), e);
                    errorOccurred.set(true); // Signal error
                } finally {
                    // Signal end of data to all writers by putting markers
                    jobLogger.info("Reader task signalling end of data to writers...");
                    try {
                        for (int i = 0; i < parallelismLevel; i++) {
                            queue.put(END_OF_DATA_MARKER); // Signal each writer task
                        }
                        jobLogger.info("End of data markers placed on queue.");
                    } catch (InterruptedException e) {
                        jobLogger.error("Reader task interrupted while placing END_OF_DATA_MARKER.");
                        Thread.currentThread().interrupt();
                        errorOccurred.set(true);
                    } catch (Exception e) {
                         jobLogger.error("Unexpected error placing END_OF_DATA_MARKER.", e);
                         errorOccurred.set(true);
                    }
                }
            });

            // --- Start Writer Tasks ---
            jobLogger.info("Starting {} parallel writer tasks...", parallelismLevel);
            for (int i = 0; i < parallelismLevel; i++) {
                final int writerId = i;
                Future<?> writerFuture = writerTaskExecutor.submit(() -> {
                    jobLogger.info("Writer task #{} started.", writerId);
                    // Each writer needs its own processor and writer instance (prototype scope)
                    MappingProcessor processor = new MappingProcessor(context); // Assuming stateless
                    try (DataWriter writer = applicationContext.getBean(config.getDestination().getType() + "Writer", DataWriter.class)) {
                        writer.open(context);
                        jobLogger.info("Writer task #{} opened its OracleJdbcWriter.", writerId);

                        int batchSize = config.getDestination().getBatchSize() > 0 ? config.getDestination().getBatchSize() : 1000;
                        List<Row> batch = new ArrayList<>(batchSize);
                        long writerRecordsProcessed = 0;

                        while (!errorOccurred.get() && !context.isCancellationRequested()) {
                            // Take row from queue, blocking if empty
                            Row row = queue.take(); // Throws InterruptedException

                            // Check for end-of-data marker
                            if (row == END_OF_DATA_MARKER) {
                                jobLogger.info("Writer task #{} received END_OF_DATA_MARKER.", writerId);
                                break; // Exit loop
                            }

                            // Process and add to batch
                            try {
                                Row processedRow = processor.process(row);
                                if (processedRow != null) { // Skip rows with handled errors
                                    batch.add(processedRow);
                                    if (batch.size() >= batchSize) {
                                        jobLogger.debug("Writer task #{} writing batch ({} rows)...", writerId, batch.size());
                                        writeBatchWithRetry(writer, batch, context); // Use retry logic
                                        writerRecordsProcessed += batch.size();
                                        context.incrementRecordsWritten(batch.size()); // Update shared counter atomically
                                        batch.clear();
                                    }
                                } else {
                                     jobLogger.debug("Writer task #{} skipping row due to handled processing error.", writerId);
                                }
                            } catch (EtlProcessingException e) {
                                // FAIL_JOB strategy error from processor
                                jobLogger.error("Writer task #{} encountered unrecoverable processing error: {}", writerId, e.getMessage(), e);
                                errorOccurred.set(true); // Signal global error
                                batch.clear(); // Discard batch
                                break; // Stop this writer
                            }
                        } // End while loop

                        // Write final partial batch if no errors occurred
                        if (!errorOccurred.get() && !context.isCancellationRequested() && !batch.isEmpty()) {
                            jobLogger.info("Writer task #{} writing final batch ({} rows)...", writerId, batch.size());
                            writeBatchWithRetry(writer, batch, context);
                            writerRecordsProcessed += batch.size();
                            context.incrementRecordsWritten(batch.size());
                            batch.clear();
                        }
                        jobLogger.info("Writer task #{} finished processing. Records written by this task: {}", writerId, writerRecordsProcessed);

                    } catch (InterruptedException e) {
                         jobLogger.warn("Writer task #{} interrupted.", writerId);
                         Thread.currentThread().interrupt();
                         errorOccurred.set(true);
                    } catch (JobCancelledException e) {
                         jobLogger.warn("Writer task #{} stopping due to cancellation request.", writerId);
                         // Don't set errorOccurred for cancellation
                    } catch (Exception e) {
                        jobLogger.error("Error during writer task #{} execution: {}", writerId, e.getMessage(), e);
                        errorOccurred.set(true); // Signal error
                    } finally {
                        writerTasksLatch.countDown(); // Signal this writer has finished
                        jobLogger.info("Writer task #{} finished and latch counted down.", writerId);
                    }
                }); // End writer task lambda
                writerFutures.add(writerFuture);
            } // End for loop starting writers

            // --- Wait for Completion ---
            jobLogger.info("Waiting for reader and writer tasks to complete...");

            // Wait for the reader to finish (or fail)
            try {
                 readerFuture.get(); // Wait for reader thread to naturally finish or throw exception
                 jobLogger.info("Reader task has completed processing.");
            } catch(ExecutionException e) {
                 jobLogger.error("Reader task failed with exception: {}", e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e.getCause());
                 errorOccurred.set(true);
            } catch(CancellationException e) {
                 jobLogger.warn("Reader task was cancelled.");
                 context.requestCancellation(); // Ensure context knows
            }

            // Wait for all writer tasks to finish processing (including END_OF_DATA)
            boolean finishedCleanly = writerTasksLatch.await(60, TimeUnit.MINUTES); // Add a timeout

            if (!finishedCleanly) {
                 jobLogger.error("Writer tasks did not finish within the timeout!");
                 errorOccurred.set(true);
                 // Attempt to cancel any lingering writer tasks
                 writerFutures.forEach(f -> f.cancel(true));
            } else {
                 jobLogger.info("All writer tasks have completed.");
            }

            // Check for errors propagated via Futures (optional, as AtomicBoolean should catch most)
            for(Future<?> future : writerFutures) {
                if (future.isDone() && !future.isCancelled()) {
                    try {
                        future.get(); // Check for exceptions thrown by writer tasks
                    } catch (ExecutionException e) {
                         jobLogger.error("A writer task failed with exception: {}", e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e.getCause());
                         errorOccurred.set(true);
                    } catch (CancellationException e) {
                         // Ignore cancellation exceptions here as they might be due to errorOccurred or timeout
                         jobLogger.warn("A writer task was cancelled.");
                    }
                }
            }


        } catch (Exception e) {
            // Catch errors during setup (reader/writer open)
            jobLogger.error("Error during ETL setup phase: {}", e.getMessage(), e);
            errorOccurred.set(true); // Signal error to potentially running tasks
            context.requestCancellation(); // Request cancellation of any potentially started tasks
            throw e; // Rethrow setup error
        } finally {
            // Shutdown reader executor
             readerExecutor.shutdown();
             try {
                if (!readerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    jobLogger.warn("Reader executor did not terminate gracefully, forcing shutdown.");
                    readerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                 readerExecutor.shutdownNow();
                 Thread.currentThread().interrupt();
            }

             // Ensure cancellation is requested if error occurred, to help tasks stop
             if (errorOccurred.get()) {
                 context.requestCancellation();
             }

            // Wait a short time for tasks to potentially react to cancellation/error
            Thread.sleep(100);
        }

        // --- Final Status Reporting ---
        long endTime = System.nanoTime();
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        if (errorOccurred.get()) {
            jobLogger.error("PARALLEL Oracle data transfer FAILED.");
            jobLogger.error("Failure Summary: Records Read: {}, Records Written: {}, Records Failed: {}, Duration: {} ms",
                           context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed(), durationMillis);
            throw new EtlProcessingException("Parallel Oracle data transfer failed for Job ID: " + context.getJobId());
        } else if (context.isCancellationRequested()) {
             jobLogger.warn("PARALLEL Oracle data transfer CANCELLED.");
             jobLogger.warn("Cancellation Summary: Records Read: {}, Records Written: {}, Records Failed: {}, Duration: {} ms",
                            context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed(), durationMillis);
             throw new JobCancelledException("Job " + context.getJobId() + " was cancelled.");
        } else {
            jobLogger.info("PARALLEL Oracle data transfer completed successfully.");
            jobLogger.info("Transfer Summary: Records Read: {}, Records Written: {}, Records Failed: {}, Duration: {} ms",
                           context.getRecordsRead(), context.getRecordsWritten(), context.getRecordsFailed(), durationMillis);
        }
    }

    // --- Helper Methods ---

    private void validateConfig(JobConfig config) {
        if (!"ORACLE_DB".equalsIgnoreCase(config.getSource().getType()) || !"ORACLE_DB".equalsIgnoreCase(config.getDestination().getType())) {
             throw new EtlProcessingException("OracleDataTransferService requires source and destination types to be 'ORACLE_DB'. Found: Source="
                                               + config.getSource().getType() + ", Dest=" + config.getDestination().getType());
        }
        if (parallelismLevel <= 0) {
             log.warn("Parallelism level configured as {} or less, defaulting to 1.", parallelismLevel);
             parallelismLevel = 1;
        }
         if (queueCapacity <= 0) {
             log.warn("Queue capacity configured as {} or less, defaulting to 1000.", queueCapacity);
             queueCapacity = 1000;
        }
    }

    private void estimateTotalRecords(DataReader reader, JobContext context) {
         try {
              long total = reader.getTotalRecords();
              context.setTotalSourceRecords(total);
              context.getLogger().info("Total source records estimated: {}", (total <= 0 ? "Unknown" : total));
         } catch (Exception e) {
              context.getLogger().warn("Could not determine total source records: {}", e.getMessage());
              context.setTotalSourceRecords(-1);
         }
    }

    /**
     * Wraps the writer's writeBatch call with retry logic.
     * (Identical to the previous version, included for completeness)
     */
    private void writeBatchWithRetry(DataWriter writer, List<Row> batch, JobContext context) throws Exception {
        int maxRetries = 3; // TODO: Make configurable
        long delayMs = 1000; // TODO: Make configurable
        int attempt = 0;
        Logger jobLogger = context.getLogger();

        while (true) {
            try {
                context.checkIfCancelled();
                writer.writeBatch(batch);
                return;
            } catch (JobCancelledException e) {
                 jobLogger.warn("Write cancelled during retry attempt.");
                 throw e;
            } catch (Exception e) {
                attempt++;
                jobLogger.warn("Write batch attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());
                if (attempt >= maxRetries || !isRetryable(e)) {
                    jobLogger.error("Write batch failed permanently after {} attempts.", attempt, e);
                    throw new EtlProcessingException("Write batch failed after " + attempt + " attempts", e);
                }
                try {
                    long currentDelay = delayMs * (long) Math.pow(2, attempt - 1);
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
     * (Identical to the previous version, included for completeness)
     */
    private boolean isRetryable(Exception e) {
        if (e instanceof java.io.IOException) return true;
        if (e instanceof java.sql.SQLTransientException) return true;
        // Add Oracle specific transient error codes
        if (e instanceof java.sql.SQLException) {
           int errorCode = ((java.sql.SQLException) e).getErrorCode();
           // Example codes (replace with actual relevant codes for your env)
           // ORA-00060: deadlock detected while waiting for resource
           // ORA-04020: deadlock detected while trying to lock object
           // ORA-01013: user requested cancel of current operation (maybe retry?)
           // ORA-03113: end-of-file on communication channel (maybe retry)
           // ORA-03114: not connected to ORACLE (maybe retry)
           // ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired
           if (errorCode == 60 || errorCode == 4020 || errorCode == 54 || errorCode == 3113 || errorCode == 3114) {
                return true;
           }
        }
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
             return isRetryable((Exception)cause); // Check cause recursively
        }
        return false;
    }


    // --- Configuration for the Writer Task Executor ---
    // This bean definition would typically go in a @Configuration class (e.g., AsyncConfig)

    /*
    @Bean(name = "writerTaskExecutor")
    public TaskExecutor writerTaskExecutor(@Value("${etl.engine.writer.parallelism:4}") int corePoolSize) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Core and Max pool size are the same - we want a fixed number of writers
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(corePoolSize);
        // No queue needed here, as we submit a fixed number of tasks at the start
        // executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("ETLWriter-");
        // Allow core threads to time out if idle (optional)
        // executor.setAllowCoreThreadTimeOut(true);
        // executor.setKeepAliveSeconds(60);
        executor.initialize();
        log.info("Configured writerTaskExecutor with core/max size: {}", corePoolSize);
        return executor;
    }
    */

}
