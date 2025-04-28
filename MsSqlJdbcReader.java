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
     * DataReader implementation for reading data from Microsoft SQL Server using JDBC.
     * Handles dynamic query building based on mappings and streams the ResultSet.
     * Marked as Prototype scope.
     */
    @Component("mssqlReader") // *** Changed Bean name ***
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public class MssqlJdbcReader implements DataReader {

        private Connection connection;
        private PreparedStatement preparedStatement;
        private ResultSet resultSet;
        private JobContext context;
        private DataSource dataSource;
        private String sqlQuery;
        private int fetchSize; // Fetch size hint for MSSQL driver
        private Logger log;
        private List<String> sourceColumnNames;
        private long totalRecords = -1;

        @Override
        public void open(JobContext context) throws Exception {
            this.context = Objects.requireNonNull(context, "JobContext cannot be null");
            this.log = context.getLogger();
            List<Mapping> mappings = context.getConfig().getMappings();
            JobConfig.SourceConfig sourceConfig = context.getConfig().getSource();
            Map<String, Object> connDetails = sourceConfig.getConnectionDetails();

            // Fetch size is a hint; MSSQL driver might handle it differently than Oracle
            this.fetchSize = ((Number) connDetails.getOrDefault("fetchSize", 1000)).intValue();
            if (this.fetchSize <= 0) this.fetchSize = 1000;

            // --- DataSource Creation/Lookup (Replace with Spring bean injection) ---
            this.dataSource = createHikariDataSource(connDetails);
            log.info("MSSQL Reader DataSource created/obtained for job {}", context.getJobId());
            // --------------------------------------------------------------------

            this.connection = dataSource.getConnection();
            log.debug("MSSQL Reader database connection obtained.");
            // AutoCommit default is usually true for MSSQL, often okay for reading.
            // Setting to false might be needed if complex reads require transaction control.
            // this.connection.setAutoCommit(false);

            this.sqlQuery = buildSelectQuery(sourceConfig, mappings);
            log.info("Executing MSSQL source query: {}", sqlQuery);

            // For MSSQL, TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are defaults, but explicit is fine.
            this.preparedStatement = connection.prepareStatement(
                sqlQuery,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY
            );

            // Set fetch size hint (effectiveness varies by driver version/settings)
            this.preparedStatement.setFetchSize(fetchSize);
            log.debug("MSSQL Reader prepared statement created with fetch size hint: {}", fetchSize);

            this.resultSet = preparedStatement.executeQuery();
            log.info("MSSQL Source query executed successfully.");

            cacheActualColumnNames(resultSet.getMetaData());

            // Optional: Calculate total records
            // estimateTotalRecords();
        }

        // Example DataSource creation (Replace with Spring bean injection)
        private DataSource createHikariDataSource(Map<String, Object> connDetails) {
            HikariConfig config = new HikariConfig();
            // Example MSSQL JDBC URL: jdbc:sqlserver://<server>:<port>;databaseName=<db>;encrypt=true;trustServerCertificate=false;
            config.setJdbcUrl((String) connDetails.get("jdbcUrl"));
            config.setUsername((String) connDetails.get("username"));

            String passwordEnvVar = (String) connDetails.get("passwordEnvVar");
            String password = null;
            if (passwordEnvVar != null) password = System.getenv(passwordEnvVar);
            if (password == null) password = (String) connDetails.get("password"); // Fallback
            if (password == null) throw new EtlProcessingException("MSSQL password not found.");
            config.setPassword(password);

            // Pool settings (tune as needed)
            config.setMaximumPoolSize(5);
            config.setMinimumIdle(1);
            // Add other HikariCP settings...

            log.info("Creating Reader HikariDataSource for MSSQL URL: {}", config.getJdbcUrl());
            return new HikariDataSource(config);
        }

        // Dynamic SQL Builder (Mostly the same as Oracle version)
        // Differences might be needed for identifier quoting ([col] vs "col") if not standard
        private String buildSelectQuery(JobConfig.SourceConfig sourceConfig, List<Mapping> mappings) {
             Map<String, Object> connDetails = sourceConfig.getConnectionDetails();
             if (connDetails.containsKey("query")) {
                 log.debug("Using provided source query.");
                 this.sourceColumnNames = null;
                 return (String) connDetails.get("query");
             } else if (connDetails.containsKey("tableName")) {
                 log.debug("Building query dynamically for table: {}", connDetails.get("tableName"));
                 String tableName = (String) connDetails.get("tableName");
                 List<String> columnsToSelect = mappings.stream()
                                                 .map(Mapping::getSourceFieldName)
                                                 .distinct()
                                                 .collect(Collectors.toList());

                 String fields;
                 if (columnsToSelect.isEmpty()) {
                     log.warn("No source fields mapped. Selecting all columns (*) from table {}.", tableName);
                     fields = "*";
                     this.sourceColumnNames = List.of("*");
                 } else {
                     // MSSQL uses square brackets for quoting by default: [column name]
                     // Standard double quotes work if QUOTED_IDENTIFIER is ON (default)
                     fields = columnsToSelect.stream()
                                             // .map(c -> "[" + c + "]") // Example MSSQL quoting
                                             .collect(Collectors.joining(", "));
                     this.sourceColumnNames = Collections.unmodifiableList(columnsToSelect);
                 }

                 // TODO: Add quoting for table name if needed: [schema].[table]
                 String query = "SELECT " + fields + " FROM " + tableName;
                 if (connDetails.containsKey("filter") && connDetails.get("filter") != null && !((String)connDetails.get("filter")).isEmpty()) {
                     query += " WHERE " + connDetails.get("filter");
                 }
                 log.debug("Dynamically built MSSQL query: {}", query);
                 return query;
             } else {
                 throw new EtlProcessingException("Source config must contain 'query' or 'tableName'.");
             }
        }

        // cacheActualColumnNames (Identical to Oracle version)
        private void cacheActualColumnNames(ResultSetMetaData metaData) throws SQLException {
            int columnCount = metaData.getColumnCount();
            List<String> actualNames = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                actualNames.add(metaData.getColumnLabel(i) != null ? metaData.getColumnLabel(i) : metaData.getColumnName(i));
            }
            this.sourceColumnNames = Collections.unmodifiableList(actualNames);
            log.debug("Cached actual source column names from ResultSetMetaData: {}", this.sourceColumnNames);
        }

        @Override
        public Stream<Row> stream() {
            ResultSetSpliterator spliterator = new ResultSetSpliterator(resultSet, context, sourceColumnNames);
            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> {
                        log.debug("MSSQL Reader stream closed, closing resources...");
                        try { close(); } catch (Exception e) { log.error("Error closing MSSQL reader resources on stream close", e); }
                    });
        }

        // ResultSetSpliterator (Inner class - Identical to Oracle version)
        private static class ResultSetSpliterator extends Spliterators.AbstractSpliterator<Row> {
             private final ResultSet rs;
             private final JobContext context;
             private final Logger log;
             private final List<String> columnNames;

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
                     if (!rs.next()) return false;
                     action.accept(mapResultSetToRow(rs, columnNames));
                     context.incrementAndGetRecordsRead();
                     return true;
                 } catch (SQLException e) {
                     log.error("SQL error reading from MSSQL ResultSet: {}", e.getMessage(), e);
                     throw new RuntimeException("SQL error reading from MSSQL ResultSet", e);
                 } catch (Exception e) {
                      log.error("Error during MSSQL ResultSet processing: {}", e.getMessage(), e);
                      throw new RuntimeException("Error during MSSQL ResultSet processing", e);
                 }
             }

             private Row mapResultSetToRow(ResultSet rs, List<String> actualColumnNames) throws SQLException {
                 Map<String, Object> data = new HashMap<>();
                 for (String columnName : actualColumnNames) {
                     Object value = rs.getObject(columnName);
                     data.put(columnName, rs.wasNull() ? null : value);
                 }
                 return new Row(data);
             }
        }

        @Override
        public long getTotalRecords() {
            // Implement count query specific to MSSQL if needed
            return this.totalRecords;
        }

        @Override
        public void close() throws Exception {
             log.debug("Closing MSSQL reader resources for job {}", context.getJobId());
             // Close rs, ps, conn similar to OracleJdbcReader, handling potential exceptions
             try {
                 if (resultSet != null && !resultSet.isClosed()) resultSet.close();
             } finally {
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
    
