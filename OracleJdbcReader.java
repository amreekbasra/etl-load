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
 * DataReader implementation for reading data from an Oracle database using JDBC.
 * Handles dynamic query building based on mappings and streams the ResultSet.
 * Marked as Prototype scope.
 */
@Component("oracledbReader") // Bean name matching factory logic
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OracleJdbcReader implements DataReader {

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private JobContext context;
    private DataSource dataSource; // Use a pooled DataSource
    private String sqlQuery;
    private int fetchSize;
    private Logger log;
    private List<String> sourceColumnNames; // Actual columns selected in the query
    private long totalRecords = -1;

    @Override
    public void open(JobContext context) throws Exception {
        this.context = Objects.requireNonNull(context, "JobContext cannot be null");
        this.log = context.getLogger();
        List<Mapping> mappings = context.getConfig().getMappings(); // Needed for dynamic query building
        JobConfig.SourceConfig sourceConfig = context.getConfig().getSource();
        Map<String, Object> connDetails = sourceConfig.getConnectionDetails();

        // Extract configuration
        this.fetchSize = ((Number) connDetails.getOrDefault("fetchSize", 1000)).intValue();
        if (this.fetchSize <= 0) {
            log.warn("Fetch size must be positive, defaulting to 1000.");
            this.fetchSize = 1000;
        }

        // --- DataSource Creation/Lookup ---
        // Replace with Spring bean injection/lookup
        this.dataSource = createHikariDataSource(connDetails);
        log.info("Reader DataSource created/obtained for job {}", context.getJobId());
        // ---------------------------------

        this.connection = dataSource.getConnection();
        log.debug("Reader database connection obtained.");
        // Setting autoCommit to false is crucial for Oracle fetch size optimization
        this.connection.setAutoCommit(false);

        // Build or get the SELECT query
        this.sqlQuery = buildSelectQuery(sourceConfig, mappings);
        log.info("Executing source query: {}", sqlQuery);

        this.preparedStatement = connection.prepareStatement(
            sqlQuery,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        );
        // Set fetch size *before* execution
        this.preparedStatement.setFetchSize(fetchSize);
        log.debug("Reader prepared statement created with fetch size: {}", fetchSize);

        // Set query parameters if needed
        // Example: preparedStatement.setString(1, "some_value");

        this.resultSet = preparedStatement.executeQuery();
        log.info("Source query executed successfully.");

        // Cache the actual column names from the ResultSet metadata
        cacheActualColumnNames(resultSet.getMetaData());

        // Optionally calculate total records
        // Consider if this is needed and the performance impact
        // this.totalRecords = calculateTotalRecords(sourceConfig, connDetails);
        // context.setTotalSourceRecords(this.totalRecords);
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
         config.setMaximumPoolSize(5);
         config.setMinimumIdle(1);
         config.setConnectionTimeout(30000);
         config.setIdleTimeout(600000);
         config.setMaxLifetime(1800000);

         // Oracle specific settings (optional)
         // config.addDataSourceProperty("oracle.jdbc.defaultRowPrefetch", String.valueOf(this.fetchSize)); // Alternative way to set fetch size

         log.info("Creating Reader HikariDataSource for URL: {}", config.getJdbcUrl());
         return new HikariDataSource(config);
     }

    // Dynamic SQL Builder (Handles selecting specific columns from mappings or using provided query)
    private String buildSelectQuery(JobConfig.SourceConfig sourceConfig, List<Mapping> mappings) {
         Map<String, Object> connDetails = sourceConfig.getConnectionDetails();
         if (connDetails.containsKey("query")) {
             log.debug("Using provided source query.");
             // If using provided query, we don't know the exact columns beforehand unless parsed
             // We will get them from ResultSetMetaData later
             this.sourceColumnNames = null; // Indicate columns will come from metadata
             return (String) connDetails.get("query");
         } else if (connDetails.containsKey("tableName")) {
             log.debug("Building query dynamically for table: {}", connDetails.get("tableName"));
             String tableName = (String) connDetails.get("tableName");

             // Determine columns to select: Use source field names from mappings
             List<String> columnsToSelect = mappings.stream()
                                             .map(Mapping::getSourceFieldName)
                                             .distinct()
                                             .collect(Collectors.toList());

             if (columnsToSelect.isEmpty()) {
                 // If no mappings provided, select all columns (or fail)
                 log.warn("No source fields specified in mappings. Selecting all columns (*) from table {}.", tableName);
                 this.sourceColumnNames = List.of("*"); // Placeholder, actual names from metadata
                 // TODO: Add quoting for table name if needed
                 String query = "SELECT * FROM " + tableName;
                  if (connDetails.containsKey("filter") && connDetails.get("filter") != null && !((String)connDetails.get("filter")).isEmpty()) {
                      query += " WHERE " + connDetails.get("filter");
                  }
                  return query;

             } else {
                 // Select only the mapped columns
                 // TODO: Add proper quoting for table and column names
                 String fields = String.join(", ", columnsToSelect);
                 String query = "SELECT " + fields + " FROM " + tableName;
                 if (connDetails.containsKey("filter") && connDetails.get("filter") != null && !((String)connDetails.get("filter")).isEmpty()) {
                     query += " WHERE " + connDetails.get("filter");
                 }
                 // Store the names we intend to select for later verification/use
                 this.sourceColumnNames = Collections.unmodifiableList(columnsToSelect);
                 log.debug("Dynamically built query: {}", query);
                 return query;
             }
         } else {
             throw new EtlProcessingException("Source config must contain either 'query' or 'tableName' connection detail.");
         }
    }

    // Caches the actual column names/labels returned by the executed query
    private void cacheActualColumnNames(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<String> actualNames = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            // Use getColumnLabel first (respects AS clauses), fallback to getColumnName
            String label = metaData.getColumnLabel(i);
            actualNames.add(label != null ? label : metaData.getColumnName(i));
        }
        // If we built the query dynamically, verify the returned columns match expected? Optional.
        if (this.sourceColumnNames != null && !this.sourceColumnNames.contains("*")) {
             // Simple check if counts match, could be more thorough
             if (this.sourceColumnNames.size() != actualNames.size()) {
                 log.warn("Mismatch between requested columns ({}) and returned columns ({}). Requested: {}, Returned: {}",
                          this.sourceColumnNames.size(), actualNames.size(), this.sourceColumnNames, actualNames);
             }
        }
        // Store the actual names from metadata for mapping
        this.sourceColumnNames = Collections.unmodifiableList(actualNames);
        log.debug("Cached actual source column names from ResultSetMetaData: {}", this.sourceColumnNames);
    }

    @Override
    public Stream<Row> stream() {
        // Use a Spliterator to wrap the ResultSet iteration
        ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet, context, sourceColumnNames);

        // Create a sequential stream, ensuring resources are closed on stream termination
        return StreamSupport.stream(spliterator, false)
                .onClose(() -> {
                    log.debug("Reader stream closed, closing reader resources for job {}", context.getJobId());
                    try {
                        close();
                    } catch (Exception e) {
                        log.error("Error closing reader resources on stream close for job {}: {}", context.getJobId(), e.getMessage(), e);
                    }
                });
    }

    // Custom Spliterator for streaming ResultSet data
    private static class ResultSetSpliterator extends Spliterators.AbstractSpliterator<Row> {
        private final ResultSet rs;
        private final JobContext context;
        private final Logger log;
        private final List<String> columnNames; // Use actual column names from metadata

        ResultSetSpliterator(ResultSet rs, JobContext context, List<String> columnNames) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
            this.rs = rs;
            this.context = context;
            this.log = context.getLogger();
            this.columnNames = Objects.requireNonNull(columnNames, "Actual column names cannot be null");
        }

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            try {
                context.checkIfCancelled();

                if (!rs.next()) {
                    log.debug("End of ResultSet reached.");
                    return false; // End of stream
                }

                // Convert current ResultSet row to a Row object
                Row row = mapResultSetToRow(rs, columnNames);
                action.accept(row);
                context.incrementAndGetRecordsRead(); // Increment after successful read
                return true;
            } catch (SQLException e) {
                log.error("SQL error reading from ResultSet for job {}: {}", context.getJobId(), e.getMessage(), e);
                throw new RuntimeException("SQL error reading from ResultSet", e);
            } catch (Exception e) {
                 log.error("Error during ResultSet processing for job {}: {}", context.getJobId(), e.getMessage(), e);
                 throw new RuntimeException("Error during ResultSet processing", e);
            }
        }

        // Maps the current ResultSet row using the actual column names from metadata
        private Row mapResultSetToRow(ResultSet rs, List<String> actualColumnNames) throws SQLException {
            Map<String, Object> data = new HashMap<>();
            for (String columnName : actualColumnNames) {
                Object value = rs.getObject(columnName);
                // Store raw value; casting/mapping happens in Processor step
                data.put(columnName, rs.wasNull() ? null : value);
            }
            return new Row(data);
        }
    }

    @Override
    public long getTotalRecords() {
        // Return cached value if calculated during open()
        // Calculation logic would be similar to the previous JdbcDataReader example
        return this.totalRecords;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Oracle reader resources for job {}", context.getJobId());
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                 resultSet.close();
                 log.debug("ResultSet closed.");
             }
        } finally {
            try {
                if (preparedStatement != null && !preparedStatement.isClosed()) {
                    preparedStatement.close();
                    log.debug("Reader PreparedStatement closed.");
                }
            } finally {
                try {
                    if (connection != null && !connection.isClosed()) {
                        // Rollback transaction if autoCommit was false (good practice for read-only)
                        try {
                            if (!connection.getAutoCommit()) {
                                connection.rollback();
                                log.debug("Reader connection rolled back (read-only safety).");
                            }
                        } catch(SQLException rbEx) {
                             log.warn("Error during reader rollback on close for job {}: {}", context.getJobId(), rbEx.getMessage());
                        } finally {
                             connection.close(); // Return connection to the pool
                             log.debug("Reader Connection closed (returned to pool).");
                        }
                    }
                } finally {
                     // Close DataSource if created here (not recommended)
                     if (dataSource instanceof HikariDataSource && !((HikariDataSource)dataSource).isClosed()) {
                          log.info("Closing temporary Reader HikariDataSource.");
                          ((HikariDataSource)dataSource).close();
                     }
                }
            }
        }
         log.debug("Finished closing Oracle reader resources for job {}", context.getJobId());
    }
}
