package com.yourcompany.etl.core.writer.impl;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Mapping;
import com.yourcompany.etl.core.model.Row;
import com.yourcompany.etl.core.writer.DataWriter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * DataWriter implementation for writing data to an Oracle database using JDBC.
 * Handles dynamic INSERT statement generation based on mappings and uses batch updates.
 * Marked as Prototype scope because each job needs its own instance/connection.
 */
@Component("oracledbWriter") // Bean name matching factory logic
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OracleJdbcWriter implements DataWriter {

    private JobContext context;
    private Logger log;
    private DataSource dataSource; // Use a pooled DataSource
    private Connection connection;
    private PreparedStatement preparedStatement;
    private String insertSql;
    private List<Mapping> mappings; // Mappings relevant to the destination
    private List<String> destinationColumns; // Ordered list of columns in the INSERT statement
    private int batchSize;
    private int currentBatchCount = 0;

    @Override
    public void open(JobContext context) throws Exception {
        this.context = Objects.requireNonNull(context, "JobContext cannot be null");
        this.log = context.getLogger();
        this.mappings = context.getConfig().getMappings(); // Get mappings from config
        JobConfig.DestinationConfig destConfig = context.getConfig().getDestination();
        Map<String, Object> connDetails = destConfig.getConnectionDetails();

        this.batchSize = destConfig.getBatchSize() > 0 ? destConfig.getBatchSize() : 1000; // Use configured batch size

        // --- DataSource Creation/Lookup ---
        // As in the reader, replace this with proper Spring bean injection/lookup
        this.dataSource = createHikariDataSource(connDetails);
        log.info("Writer DataSource created/obtained for job {}", context.getJobId());
        // ---------------------------------

        this.connection = dataSource.getConnection();
        this.connection.setAutoCommit(false); // Disable auto-commit for batching and transaction control
        log.debug("Writer database connection obtained, autoCommit set to false.");

        // Build dynamic INSERT statement
        this.insertSql = buildInsertSql(connDetails, mappings);
        log.info("Prepared insert statement SQL: {}", this.insertSql);

        this.preparedStatement = connection.prepareStatement(insertSql);
        log.debug("Writer PreparedStatement created.");
    }

    // Example DataSource creation (Replace with Spring bean injection)
    private DataSource createHikariDataSource(Map<String, Object> connDetails) {
         HikariConfig config = new HikariConfig();
         config.setJdbcUrl((String) connDetails.get("jdbcUrl"));
         config.setUsername((String) connDetails.get("username"));
         // IMPORTANT: Use environment variables or a secure vault for passwords!
         String passwordEnvVar = (String) connDetails.get("passwordEnvVar"); // Expect env var name in config
         String password = null;
         if (passwordEnvVar != null) {
            password = System.getenv(passwordEnvVar);
         }
         if (password == null) {
             password = (String) connDetails.get("password"); // Fallback (less secure)
             if(password != null) log.warn("Using password directly from config is insecure. Use environment variables or secrets management via 'passwordEnvVar'.");
             else throw new EtlProcessingException("Database password environment variable name not set in 'passwordEnvVar' or password not found in config.");
         }
         config.setPassword(password);

         // Add common pool settings (tune these)
         config.setMaximumPoolSize(10); // Can be slightly larger for writers if needed
         config.setMinimumIdle(2);
         config.setConnectionTimeout(30000);
         config.setIdleTimeout(600000);
         config.setMaxLifetime(1800000);

         // Oracle specific optimization (optional, test impact)
         // config.addDataSourceProperty("oracle.jdbc.implicitStatementCacheSize", "10");

         log.info("Creating Writer HikariDataSource for URL: {}", config.getJdbcUrl());
         return new HikariDataSource(config);
     }


    private String buildInsertSql(Map<String, Object> connDetails, List<Mapping> mappings) {
        String tableName = (String) connDetails.get("tableName");
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new EtlProcessingException("Missing 'tableName' in destination connectionDetails for Oracle writer.");
        }

        // Use destination field names from mappings
        this.destinationColumns = mappings.stream()
                                          .map(Mapping::getDestinationFieldName)
                                          .collect(Collectors.toList());

        if (this.destinationColumns.isEmpty()) {
            // Option 1: Fail if no columns are mapped
            throw new EtlProcessingException("No destination columns found in mappings. Cannot build INSERT statement.");
            // Option 2: Try to insert into all columns (requires fetching table metadata - more complex)
            // log.warn("No destination columns mapped. Attempting insert into all columns (requires metadata fetch - not implemented).");
            // fetchTableMetadataAndSetColumns(tableName); // Placeholder for more advanced logic
        }

        String columnsPart = this.destinationColumns.stream()
                                   // TODO: Add quoting for column names if needed (e.g., if they contain special chars or are case-sensitive)
                                   // .map(col -> "\"" + col + "\"")
                                   .collect(Collectors.joining(", "));

        String valuesPart = this.destinationColumns.stream()
                                  .map(col -> "?")
                                  .collect(Collectors.joining(", "));

        // TODO: Add quoting for table name if needed
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnsPart, valuesPart);
    }

    @Override
    public void writeBatch(List<Row> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            log.debug("Received empty batch, nothing to write.");
            return;
        }
        log.debug("Adding {} rows to current batch.", batch.size());

        for (Row row : batch) {
            context.checkIfCancelled(); // Check for cancellation before processing row

            try {
                // Set parameters for the prepared statement based on destination column order
                int paramIndex = 1;
                for (String colName : this.destinationColumns) {
                    Object value = row.getValue(colName); // Get value using destination name

                    // TODO: Potentially more robust type handling/conversion if needed before setXXX
                    // For Oracle, setObject is often flexible, but explicit setNull or setTimestamp etc. might be required sometimes.
                    if (value == null) {
                        // Need to find the SQL type from mapping or metadata if possible
                        // For simplicity, using Types.NULL, but might need specific type for some drivers/DBs
                        // preparedStatement.setNull(paramIndex++, findSqlType(colName)); // More robust
                         preparedStatement.setObject(paramIndex++, null); // Simpler, often works
                    } else {
                        preparedStatement.setObject(paramIndex++, value);
                    }
                }
                preparedStatement.addBatch();
                currentBatchCount++;
            } catch (SQLException e) {
                log.error("Error setting parameters or adding row to batch for job {}: Row Data: {}", context.getJobId(), row, e);
                // Handle error - potentially rollback, log error row, and continue or fail based on config
                // This simple version will let the exception propagate to executeBatch
                throw e;
            }

            // Execute batch if size limit is reached
            if (currentBatchCount >= batchSize) {
                executeCurrentBatch();
            }
        }
        // Note: The final batch execution happens either when the batch size is reached
        // or during the close() method to ensure remaining records are written.
    }

    // Executes the currently accumulated batch
    private void executeCurrentBatch() throws SQLException {
        if (currentBatchCount == 0) {
            log.debug("No records in current batch to execute.");
            return;
        }

        log.debug("Executing batch of {} records...", currentBatchCount);
        long startTime = System.nanoTime();
        try {
            int[] updateCounts = preparedStatement.executeBatch();
            long endTime = System.nanoTime();
            log.debug("Batch execution completed in {} ms.", TimeUnit.NANOSECONDS.toMillis(endTime - startTime));

            // Optional: Check update counts for errors if needed (e.g., Statement.EXECUTE_FAILED)
            int successfulRows = 0;
            for (int count : updateCounts) {
                if (count >= 0 || count == Statement.SUCCESS_NO_INFO) {
                    successfulRows++;
                } else if (count == Statement.EXECUTE_FAILED) {
                    log.error("Batch execution failed for at least one row in the batch for job {}.", context.getJobId());
                    // This indicates a failure, but JDBC batching might not tell you *which* row failed easily.
                    // Need to handle this failure case - potentially requires stopping or more detailed logging.
                }
            }
            if(successfulRows != currentBatchCount) {
                 log.warn("Batch execution reported success for {} rows, but expected {}. Potential issues occurred.", successfulRows, currentBatchCount);
                 // Consider throwing an exception or specific handling based on requirements
            } else {
                 log.debug("Successfully executed batch of {} rows.", currentBatchCount);
            }

            connection.commit(); // Commit the transaction after successful batch execution
            log.debug("Transaction committed for batch.");
            currentBatchCount = 0; // Reset batch count

        } catch (BatchUpdateException bue) {
            log.error("Batch update failed for job {}: {}", context.getJobId(), bue.getMessage(), bue);
            log.error("Batch Update Counts: {}", bue.getUpdateCounts()); // Log counts to see where it failed
            // Attempt to rollback the transaction
            rollbackConnection("batch update failure");
            // Rethrow or handle based on strategy. This is usually a critical error.
            throw bue;
        } catch (SQLException e) {
            log.error("SQL error during batch execution or commit for job {}: {}", context.getJobId(), e.getMessage(), e);
            rollbackConnection("batch execution SQL error");
            throw e;
        } catch (Exception e) {
             log.error("Unexpected error during batch execution for job {}: {}", context.getJobId(), e.getMessage(), e);
             rollbackConnection("batch execution unexpected error");
             throw e;
        }
    }

    // Helper method to rollback the connection safely
    private void rollbackConnection(String reason) {
        if (connection != null) {
            try {
                log.warn("Rolling back transaction due to: {}", reason);
                connection.rollback();
                log.info("Transaction rolled back successfully.");
            } catch (SQLException rollbackEx) {
                log.error("Failed to rollback transaction for job {}: {}", context.getJobId(), rollbackEx.getMessage(), rollbackEx);
            }
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Closing Oracle writer for job {}", context.getJobId());
        try {
            // Execute any remaining records in the last batch
            if (currentBatchCount > 0) {
                log.info("Executing final batch of {} records before closing...", currentBatchCount);
                executeCurrentBatch();
            } else {
                 // If the last action was a successful batch execute, the commit happened there.
                 // If no records were ever added, no commit is needed.
                 // If open failed, connection might be null.
                 // If the last batch failed, rollback happened there.
                 // It might be safest to commit here *if* the connection is open and not in error,
                 // but executeCurrentBatch handles commits, so it might be redundant or hide issues.
                 log.debug("No pending records in the final batch.");
            }
        } catch (Exception e) {
             log.error("Error executing final batch during close for job {}: {}", context.getJobId(), e.getMessage(), e);
             rollbackConnection("final batch execution failure");
             // Decide if this error should prevent closing other resources. Usually, we still try to close.
             // throw e; // Optionally rethrow
        } finally {
            // Close PreparedStatement
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                    log.debug("Writer PreparedStatement closed.");
                } catch (SQLException e) {
                    log.error("Error closing writer PreparedStatement for job {}: {}", context.getJobId(), e.getMessage(), e);
                }
            }
            // Close Connection (returns to pool)
            if (connection != null) {
                try {
                    // Final check: if autoCommit was false and no errors occurred,
                    // ensure the last transaction state is handled (commit/rollback).
                    // executeCurrentBatch handles commits, rollbackConnection handles rollbacks on error.
                    connection.close();
                    log.debug("Writer Connection closed (returned to pool).");
                } catch (SQLException e) {
                    log.error("Error closing writer Connection for job {}: {}", context.getJobId(), e.getMessage(), e);
                }
            }
             // Close DataSource if created here (not recommended - Spring should manage beans)
             if (dataSource instanceof HikariDataSource && !((HikariDataSource)dataSource).isClosed()) {
                  log.info("Closing temporary Writer HikariDataSource.");
                  ((HikariDataSource)dataSource).close();
             }
        }
         log.info("Finished closing Oracle writer for job {}", context.getJobId());
    }
}
