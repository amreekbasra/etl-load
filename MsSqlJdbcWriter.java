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
    import java.util.concurrent.TimeUnit;
    import java.util.stream.Collectors;

    /**
     * DataWriter implementation for writing data to Microsoft SQL Server using JDBC batch updates.
     * Marked as Prototype scope.
     */
    @Component("mssqlWriter") // *** Changed Bean name ***
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public class MssqlJdbcWriter implements DataWriter {

        private JobContext context;
        private Logger log;
        private DataSource dataSource;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private String insertSql;
        private List<String> destinationColumns; // Ordered list of columns for INSERT
        private int batchSize;
        private int currentBatchCount = 0;

        @Override
        public void open(JobContext context) throws Exception {
            this.context = Objects.requireNonNull(context, "JobContext cannot be null");
            this.log = context.getLogger();
            List<Mapping> mappings = context.getConfig().getMappings();
            JobConfig.DestinationConfig destConfig = context.getConfig().getDestination();
            Map<String, Object> connDetails = destConfig.getConnectionDetails();

            this.batchSize = destConfig.getBatchSize() > 0 ? destConfig.getBatchSize() : 1000;

            // --- DataSource Creation/Lookup (Replace with Spring bean injection) ---
            this.dataSource = createHikariDataSource(connDetails);
            log.info("MSSQL Writer DataSource created/obtained for job {}", context.getJobId());
            // --------------------------------------------------------------------

            this.connection = dataSource.getConnection();
            this.connection.setAutoCommit(false); // Disable auto-commit for batching
            log.debug("MSSQL Writer database connection obtained, autoCommit=false.");

            this.insertSql = buildInsertSql(connDetails, mappings);
            log.info("Prepared MSSQL insert statement SQL: {}", this.insertSql);

            this.preparedStatement = connection.prepareStatement(insertSql);
            log.debug("MSSQL Writer PreparedStatement created.");
        }

        // Example DataSource creation (Replace with Spring bean injection)
        private DataSource createHikariDataSource(Map<String, Object> connDetails) {
             HikariConfig config = new HikariConfig();
             config.setJdbcUrl((String) connDetails.get("jdbcUrl"));
             config.setUsername((String) connDetails.get("username"));

             String passwordEnvVar = (String) connDetails.get("passwordEnvVar");
             String password = null;
             if (passwordEnvVar != null) password = System.getenv(passwordEnvVar);
             if (password == null) password = (String) connDetails.get("password"); // Fallback
             if (password == null) throw new EtlProcessingException("MSSQL password not found.");
             config.setPassword(password);

             // Pool settings (tune as needed)
             config.setMaximumPoolSize(10); // Adjust based on parallelism/load
             config.setMinimumIdle(2);
             // Add other HikariCP settings...

             log.info("Creating Writer HikariDataSource for MSSQL URL: {}", config.getJdbcUrl());
             return new HikariDataSource(config);
         }

        // Build INSERT SQL (Mostly same as Oracle, check quoting if needed)
        private String buildInsertSql(Map<String, Object> connDetails, List<Mapping> mappings) {
            String tableName = (String) connDetails.get("tableName");
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new EtlProcessingException("Missing 'tableName' in destination connectionDetails for MSSQL writer.");
            }

            this.destinationColumns = mappings.stream()
                                              .map(Mapping::getDestinationFieldName)
                                              .collect(Collectors.toList());

            if (this.destinationColumns.isEmpty()) {
                throw new EtlProcessingException("No destination columns mapped for MSSQL writer.");
            }

            // Use standard SQL quoting or MSSQL specific ([col]) if necessary
            String columnsPart = this.destinationColumns.stream()
                                       // .map(c -> "[" + c + "]") // Example MSSQL quoting
                                       .collect(Collectors.joining(", "));
            String valuesPart = this.destinationColumns.stream().map(c -> "?").collect(Collectors.joining(", "));

            // TODO: Add schema/table quoting if needed: [schema].[table]
            return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnsPart, valuesPart);
        }

        @Override
        public void writeBatch(List<Row> batch) throws Exception {
            if (batch == null || batch.isEmpty()) return;
            log.debug("Adding {} rows to current MSSQL batch.", batch.size());

            for (Row row : batch) {
                context.checkIfCancelled();
                try {
                    int paramIndex = 1;
                    for (String colName : this.destinationColumns) {
                        Object value = row.getValue(colName);
                        // setObject is generally flexible with MSSQL JDBC driver
                        preparedStatement.setObject(paramIndex++, value);
                    }
                    preparedStatement.addBatch();
                    currentBatchCount++;
                } catch (SQLException e) {
                    log.error("Error setting params or adding row to MSSQL batch: Row: {}", row, e);
                    throw e; // Let executeBatch handle/report
                }

                if (currentBatchCount >= batchSize) {
                    executeCurrentBatch();
                }
            }
        }

        // Executes the current batch (Identical logic to Oracle version)
        private void executeCurrentBatch() throws SQLException {
            if (currentBatchCount == 0) return;
            log.debug("Executing MSSQL batch of {} records...", currentBatchCount);
            long startTime = System.nanoTime();
            try {
                int[] updateCounts = preparedStatement.executeBatch();
                // Optional: Check update counts
                connection.commit(); // Commit transaction
                log.debug("MSSQL batch executed and committed successfully ({} rows).", currentBatchCount);
                currentBatchCount = 0;
            } catch (BatchUpdateException bue) {
                log.error("MSSQL Batch update failed: {}", bue.getMessage(), bue);
                log.error("Update Counts: {}", Arrays.toString(bue.getUpdateCounts()));
                rollbackConnection("batch update failure");
                throw bue;
            } catch (SQLException e) {
                log.error("SQL error during MSSQL batch execution or commit: {}", e.getMessage(), e);
                rollbackConnection("batch execution SQL error");
                throw e;
            } catch (Exception e) {
                 log.error("Unexpected error during MSSQL batch execution: {}", e.getMessage(), e);
                 rollbackConnection("batch execution unexpected error");
                 throw e;
            }
        }

        // rollbackConnection (Identical logic to Oracle version)
        private void rollbackConnection(String reason) {
            if (connection != null) {
                try {
                    log.warn("Rolling back MSSQL transaction due to: {}", reason);
                    connection.rollback();
                    log.info("MSSQL Transaction rolled back.");
                } catch (SQLException rollbackEx) {
                    log.error("Failed to rollback MSSQL transaction", rollbackEx);
                }
            }
        }

        @Override
        public void close() throws Exception {
            log.info("Closing MSSQL writer for job {}", context.getJobId());
            try {
                if (currentBatchCount > 0) {
                    log.info("Executing final MSSQL batch of {} records...", currentBatchCount);
                    executeCurrentBatch();
                }
            } catch (Exception e) {
                 log.error("Error executing final MSSQL batch during close", e);
                 rollbackConnection("final batch execution failure");
            } finally {
                 // Close ps, conn similar to OracleJdbcWriter, handling potential exceptions
                 try {
                     if (preparedStatement != null && !preparedStatement.isClosed()) preparedStatement.close();
                 } finally {
                     try {
                         if (connection != null && !connection.isClosed()) connection.close(); // Return to pool
                     } finally {
                          if (dataSource instanceof HikariDataSource && !((HikariDataSource)dataSource).isClosed()) {
                               ((HikariDataSource)dataSource).close(); // Close if created locally
                          }
                     }
                 }
            }
        }
    }
    
