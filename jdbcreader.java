package com.yourcompany.etl.core.reader.impl;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Mapping;
import com.yourcompany.etl.core.model.Row;
import com.yourcompany.etl.core.reader.DataReader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * DataReader implementation for reading data from a JDBC source.
 * Handles dynamic query building (basic) and streaming ResultSet.
 * NOTE: Marked as Prototype scope because each job needs its own instance
 * with its own connection, statement, and result set.
 */
@Component("oracledbReader") // Bean name matching factory logic (adjust for other DBs like "mssqlReader")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JdbcDataReader implements DataReader {

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private List<Mapping> mappings;
    private JobContext context;
    private DataSource dataSource; // Use a pooled DataSource
    private String sqlQuery;
    private int fetchSize;
    private Logger log; // Job-specific logger
    private List<String> sourceColumnNames; // Store the names of columns being selected
    private long totalRecords = -1; // Cache total records

    @Override
    public void open(JobContext context) throws Exception {
        this.context = context;
        this.log = context.getLogger(); // Get job-specific logger
        this.mappings = context.getConfig().getMappings();
        JobConfig.SourceConfig sourceConfig = context.getConfig().getSource();
        Map<String, Object> connDetails = sourceConfig.getConnectionDetails();

        // Extract configuration
        this.fetchSize = ((Number) connDetails.getOrDefault("fetchSize", 1000)).intValue();
        if (this.fetchSize <= 0) {
            log.warn("Fetch size must be positive, defaulting to 1000.");
            this.fetchSize = 1000;
        }

        // --- DataSource Creation/Lookup ---
        // In a real app, use a DataSource bean managed by Spring.
        // This example creates one directly for simplicity, but it's NOT recommended
        // as it bypasses proper pooling configuration via Spring.
        this.dataSource = createHikariDataSource(connDetails);
        log.info("DataSource created/obtained for job {}", context.getJobId());
        // ---------------------------------

        this.connection = dataSource.getConnection();
        log.debug("Database connection obtained.");
        // Important for streaming large results with fetch size in some drivers (e.g., Oracle)
        this.connection.setAutoCommit(false);

        this.sqlQuery = buildSelectQuery(sourceConfig, mappings);
        log.info("Executing source query: {}", sqlQuery); // Log the query being executed

        this.preparedStatement = connection.prepareStatement(
            sqlQuery,
            ResultSet.TYPE_FORWARD_ONLY,      // Optimize for forward reading
            ResultSet.CONCUR_READ_ONLY        // No updates needed
        );
        this.preparedStatement.setFetchSize(fetchSize); // Crucial hint for the driver to stream results
        log.debug("Prepared statement created with fetch size: {}", fetchSize);

        // Set query parameters if needed (e.g., based on filters not directly in the query string)
        // Example: preparedStatement.setString(1, "some_value");

        this.resultSet = preparedStatement.executeQuery();
        log.info("Source query executed successfully.");

        // Cache the column names from the ResultSet metadata for mapping
        cacheSourceColumnNames(resultSet.getMetaData());

        // Optionally calculate total records (can be slow)
        // Consider making this configurable (e.g., "calculateTotal": true in config)
        // this.totalRecords = calculateTotalRecords(sourceConfig, connDetails);
        // context.setTotalSourceRecords(this.totalRecords);
    }

    // Example DataSource creation (Replace with Spring bean injection)
    private DataSource createHikariDataSource(Map<String, Object> connDetails) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl((String) connDetails.get("jdbcUrl"));
        config.setUsername((String) connDetails.get("username"));
        // IMPORTANT: Use environment variables or a secure vault for passwords!
        String password = System.getenv((String) connDetails.get("password")); // Example reading env var
        if (password == null) {
             password = (String) connDetails.get("password"); // Fallback (less secure)
             if(password != null) log.warn("Using password directly from config is insecure. Use environment variables or secrets management.");
             else throw new EtlProcessingException("Database password environment variable not set or found in config.");
        }
        config.setPassword(password);

        // Add common pool settings (tune these)
        config.setMaximumPoolSize(5); // Smaller pool per reader instance is often fine
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000); // 30 seconds
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes

        // Add driver-specific properties if needed
        // config.addDataSourceProperty("cachePrepStmts", "true");
        // config.addDataSourceProperty("prepStmtCacheSize", "250");

        log.info("Creating HikariDataSource for URL: {}", config.getJdbcUrl());
        return new HikariDataSource(config);
    }


    // Dynamic SQL Builder (Simplified Example)
    private String buildSelectQuery(JobConfig.SourceConfig sourceConfig, List<Mapping> mappings) {
         Map<String, Object> connDetails = sourceConfig.getConnectionDetails();
         if (connDetails.containsKey("query")) {
             log.debug("Using provided source query.");
             return (String) connDetails.get("query");
         } else if (connDetails.containsKey("tableName")) {
             log.debug("Building query dynamically for table: {}", connDetails.get("tableName"));
             String tableName = (String) connDetails.get("tableName");
             // Ensure source field names are collected correctly
             this.sourceColumnNames = mappings.stream()
                                             .map(Mapping::getSourceFieldName)
                                             .distinct() // Avoid duplicates if mapped multiple times
                                             .collect(Collectors.toList());

             if (this.sourceColumnNames.isEmpty()) {
                 throw new EtlProcessingException("No source fields specified in mappings for dynamic query building.");
             }

             // TODO: Add proper quoting for table and column names if they contain special characters or are case-sensitive
             String fields = String.join(", ", this.sourceColumnNames);
             String query = "SELECT " + fields + " FROM " + tableName;

             if (connDetails.containsKey("filter") && connDetails.get("filter") != null && !((String)connDetails.get("filter")).isEmpty()) {
                 query += " WHERE " + connDetails.get("filter");
                 log.debug("Adding filter clause: {}", connDetails.get("filter"));
             }
             log.debug("Dynamically built query: {}", query);
             return query;
         } else {
             throw new EtlProcessingException("Source config must contain either 'query' or 'tableName' connection detail.");
         }
    }

    private void cacheSourceColumnNames(ResultSetMetaData metaData) throws SQLException {
        if (this.sourceColumnNames == null) { // Only if not built dynamically
            int columnCount = metaData.getColumnCount();
            List<String> names = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                // Use getColumnLabel to respect AS clauses, fallback to getColumnName
                names.add(metaData.getColumnLabel(i) != null ? metaData.getColumnLabel(i) : metaData.getColumnName(i));
            }
            this.sourceColumnNames = Collections.unmodifiableList(names);
            log.debug("Cached source column names from ResultSetMetaData: {}", this.sourceColumnNames);
        }
    }

    @Override
    public Stream<Row> stream() {
        // Use a Spliterator to wrap the ResultSet iteration for streaming API
        ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet, mappings, context, sourceColumnNames);

        // Create a sequential stream from the spliterator
        // Ensure resources are closed when the stream finishes (successfully or exceptionally)
        return StreamSupport.stream(spliterator, false) // 'false' for sequential stream
                .onClose(() -> {
                    log.debug("Stream closed, closing reader resources for job {}", context.getJobId());
                    try {
                        close(); // Close DB resources when stream is closed
                    } catch (Exception e) {
                        // Log error, but don't throw from onClose typically
                        log.error("Error closing reader resources on stream close for job {}: {}", context.getJobId(), e.getMessage(), e);
                    }
                });
    }

    // Custom Spliterator for ResultSet streaming
    private static class ResultSetSpliterator extends Spliterators.AbstractSpliterator<Row> {
        private final ResultSet rs;
        private final List<Mapping> mappings; // Mappings needed if mapping done here
        private final JobContext context;
        private final Logger log;
        private final List<String> columnNames; // Use cached column names

        ResultSetSpliterator(ResultSet rs, List<Mapping> mappings, JobContext context, List<String> columnNames) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
            this.rs = rs;
            this.mappings = mappings; // Pass mappings if needed
            this.context = context;
            this.log = context.getLogger();
            this.columnNames = Objects.requireNonNull(columnNames, "Column names cannot be null for ResultSetSpliterator");
        }

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            try {
                context.checkIfCancelled(); // Check for cancellation before advancing

                if (!rs.next()) {
                    log.debug("End of ResultSet reached.");
                    return false; // End of stream
                }

                // Convert ResultSet row to your Row object
                Row row = mapResultSetToRow(rs, columnNames);
                action.accept(row);
                context.incrementAndGetRecordsRead(); // Increment counter *after* successful read
                return true;
            } catch (SQLException e) {
                log.error("SQL error reading from ResultSet for job {}: {}", context.getJobId(), e.getMessage(), e);
                // Let the exception propagate to fail the stream/job
                throw new RuntimeException("SQL error reading from ResultSet", e);
            } catch (Exception e) { // Catch other potential errors during mapping or cancellation check
                 log.error("Error during ResultSet processing for job {}: {}", context.getJobId(), e.getMessage(), e);
                 throw new RuntimeException("Error during ResultSet processing", e);
            }
        }

        // Maps current ResultSet row to a Row object using cached column names
        private Row mapResultSetToRow(ResultSet rs, List<String> columnNames) throws SQLException {
            Map<String, Object> data = new HashMap<>();
            for (String columnName : columnNames) {
                try {
                    Object value = rs.getObject(columnName);
                    // Store the raw value as fetched from DB; casting happens in the Processor step
                    data.put(columnName, rs.wasNull() ? null : value);
                } catch (SQLException e) {
                    // This might happen if a column name derived from mappings isn't actually in the SELECT list
                    log.warn("Column '{}' not found in ResultSet or error fetching for job {}. Setting null. SQLState: {}",
                             columnName, context.getJobId(), e.getSQLState(), e);
                    data.put(columnName, null); // Or throw based on configuration?
                }
            }
            return new Row(data);
        }
    }


    @Override
    public long getTotalRecords() {
        // Return cached value if calculated during open()
        return this.totalRecords;
    }

    // Example: Method to calculate total records (potentially slow)
    private long calculateTotalRecords(JobConfig.SourceConfig sourceConfig, Map<String, Object> connDetails) {
        if (!connDetails.containsKey("tableName")) {
             log.warn("Cannot calculate total records without a 'tableName' in source config.");
             return -1;
        }
        String tableName = (String) connDetails.get("tableName");
        String countQuery = "SELECT COUNT(*) FROM " + tableName;
         if (connDetails.containsKey("filter") && connDetails.get("filter") != null && !((String)connDetails.get("filter")).isEmpty()) {
             countQuery += " WHERE " + connDetails.get("filter");
         }

        log.info("Executing count query for total records: {}", countQuery);
        try (Connection countConn = dataSource.getConnection(); // Use separate connection
             PreparedStatement countStmt = countConn.prepareStatement(countQuery);
             ResultSet countRs = countStmt.executeQuery()) {

            if (countRs.next()) {
                long total = countRs.getLong(1);
                log.info("Total records estimated by count query: {}", total);
                return total;
            } else {
                 log.warn("Count query did not return any results.");
                 return -1;
            }
        } catch (SQLException e) {
            log.error("Failed to execute count query for job {}: {}", context.getJobId(), e.getMessage(), e);
            return -1; // Indicate unknown total on error
        }
    }


    @Override
    public void close() throws Exception {
        log.debug("Closing JDBC resources for job {}", context.getJobId());
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                 resultSet.close();
                 log.debug("ResultSet closed.");
             }
        } catch (SQLException e) {
             log.error("Error closing ResultSet for job {}: {}", context.getJobId(), e.getMessage(), e);
             // Log but continue closing other resources
        } finally {
            try {
                if (preparedStatement != null && !preparedStatement.isClosed()) {
                    preparedStatement.close();
                    log.debug("PreparedStatement closed.");
                }
            } catch (SQLException e) {
                 log.error("Error closing PreparedStatement for job {}: {}", context.getJobId(), e.getMessage(), e);
            } finally {
                try {
                    if (connection != null && !connection.isClosed()) {
                        // Rollback any potential changes if autoCommit was false (though we only read)
                        try {
                            if (!connection.getAutoCommit()) {
                                connection.rollback(); // Good practice, though likely no changes
                                log.debug("Connection rolled back (read-only safety).");
                            }
                        } catch(SQLException rbEx) {
                             log.warn("Error during rollback on close for job {}: {}", context.getJobId(), rbEx.getMessage());
                        } finally {
                             connection.close(); // Return connection to the pool
                             log.debug("Connection closed (returned to pool).");
                        }
                    }
                } catch (SQLException e) {
                     log.error("Error closing Connection for job {}: {}", context.getJobId(), e.getMessage(), e);
                     // If using HikariDataSource created here, explicitly close it if needed,
                     // but typically Spring manages the lifecycle of DataSource beans.
                     if (dataSource instanceof HikariDataSource && !((HikariDataSource)dataSource).isClosed()) {
                          log.info("Closing temporary HikariDataSource.");
                          ((HikariDataSource)dataSource).close();
                     }
                }
            }
        }
         log.debug("Finished closing JDBC resources for job {}", context.getJobId());
    }
}
