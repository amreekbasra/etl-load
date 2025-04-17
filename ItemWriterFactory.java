package com.example.etl.job;

import com.example.etl.model.DestinationType;
import com.example.etl.model.EtlTaskConfig;
import com.example.etl.model.FieldMetadata;
import com.example.etl.service.DynamicJobService;
import com.example.etl.util.JdbcTypeHandler; // Assuming you have this utility
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
// Consider FlatFileItemWriterBuilder for file output
// import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
// import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
// import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils; // Import StringUtils

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory responsible for creating step-scoped ItemWriter beans
 * based on the dynamic EtlTaskConfig.
 */
@Component // Factory itself is a singleton managed by Spring
public class ItemWriterFactory {
    private static final Logger log = LoggerFactory.getLogger(ItemWriterFactory.class);

    @Autowired
    private DynamicJobService dynamicJobService; // Service to retrieve the full task config

    // Inject available DataSources
    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource oracleDataSource;

    @Autowired
    @Qualifier("msSqlDataSource")
    private DataSource msSqlDataSource;

    @Autowired // Inject the type handler utility bean
    private JdbcTypeHandler jdbcTypeHandler;

    /**
     * Defines the step-scoped bean for the dynamic ItemWriter.
     *
     * @param configKey Injected from JobParameters via @Value - identifies the config for this run.
     * @return A configured ItemWriter instance for the specific step execution.
     * @throws IllegalArgumentException if the configuration is invalid or destination type unsupported.
     */
    @Bean(name = "dynamicItemWriter")
    @Scope(value = "step", proxyMode = ScopedProxyMode.INTERFACES) // Proxy needed as it returns an interface
    public ItemWriter<Map<String, Object>> createWriter(
            @Value("#{jobParameters['configKey']}") String configKey // Inject from Job Params
    ) {
        log.info("Creating step-scoped ItemWriter for configKey: {}", configKey);
        // Retrieve the full configuration using the key
        EtlTaskConfig config = dynamicJobService.getTaskConfig(configKey);
        Map<String, String> destDetails = config.getDestinationDetails();
        DestinationType destType = config.getDestinationType();

        if (destDetails == null) {
             throw new IllegalArgumentException("Destination details are missing in EtlTaskConfig for key: " + configKey);
        }

        try {
            switch (destType) {
                case ORACLE_DB:
                    log.debug("Building Oracle DB ItemWriter for key: {}", configKey);
                    return buildJdbcWriter(oracleDataSource, destDetails, config.getFieldMappings(), "oracleWriter_" + configKey);
                case MSSQL_DB:
                    log.debug("Building MS SQL DB ItemWriter for key: {}", configKey);
                    return buildJdbcWriter(msSqlDataSource, destDetails, config.getFieldMappings(), "mssqlWriter_" + configKey);
                // Add cases for FILE_CSV, API_REST etc.
                // case FILE_CSV:
                //    return buildCsvFileWriter(...)
                default:
                    log.error("Unsupported destination type [{}] specified for writer in configKey: {}", destType, configKey);
                    throw new IllegalArgumentException("Unsupported destination type for writer: " + destType);
            }
        } catch (Exception e) {
            log.error("Failed to create ItemWriter for configKey [{}], DestinationType [{}]: {}", configKey, destType, e.getMessage(), e);
            // Propagate exception to fail the step initialization
            throw new RuntimeException("Failed to create ItemWriter for " + destType, e);
        }
    }

    /**
     * Builds a configured JdbcBatchItemWriter for database destinations.
     *
     * @param dataSource The DataSource for the target database.
     * @param details    Map containing destination details like "tableName".
     * @param mappings   List of FieldMetadata defining the target columns and types.
     * @param writerName A unique name for the writer bean.
     * @return A configured JdbcBatchItemWriter.
     */
    private ItemWriter<Map<String, Object>> buildJdbcWriter(DataSource dataSource, Map<String, String> details, List<FieldMetadata> mappings, String writerName) {
        String tableName = details.get("tableName");

        if (!StringUtils.hasText(tableName)) {
            throw new IllegalArgumentException("tableName is required in destinationDetails for DB writer.");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("fieldMappings are required for DB writer to map columns.");
        }

        // Generate SQL: INSERT INTO table (destCol1, destCol2, ...) VALUES (?, ?, ...)
        // Ensure columns are ordered according to the mappings list for PreparedStatement parameter setting.
        String insertColumns = mappings.stream()
                .map(FieldMetadata::getDestFieldName)
                .filter(StringUtils::hasText)
                .collect(Collectors.joining(", "));

        String placeholders = mappings.stream()
                .map(f -> "?")
                .collect(Collectors.joining(", "));

        if (!StringUtils.hasText(insertColumns) || !StringUtils.hasText(placeholders) || mappings.isEmpty()) {
             throw new IllegalArgumentException("No valid destination field names found in mappings for DB writer.");
        }

        // Basic validation/sanitization of table name
        String safeTableName = tableName.replaceAll("[^a-zA-Z0-9_.]", "");
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", safeTableName, insertColumns, placeholders);

        log.info("Building JDBC Writer [{}] with SQL: {}", writerName, sql);

        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(sql)
                // Use ItemPreparedStatementSetter to map values from the input Map to the SQL placeholders.
                // This gives precise control over parameter order and type handling.
                .itemPreparedStatementSetter((itemMap, ps) -> {
                    // The input 'itemMap' keys *must* correspond to the DESTINATION field names
                    // as defined in the FieldMetadata, because the reader should have mapped
                    // source names to destination names before this point (or processor).
                    if (itemMap == null) {
                         log.warn("Received null item map in ItemPreparedStatementSetter for SQL: {}", sql);
                         // Decide how to handle null items - skip? throw error?
                         // For now, let it potentially cause a NullPointerException below if accessed.
                         return; // Or throw new IllegalArgumentException("Cannot process null item map");
                    }

                    for (int i = 0; i < mappings.size(); i++) {
                        FieldMetadata meta = mappings.get(i);
                        String destFieldName = meta.getDestFieldName();
                        Object value = itemMap.get(destFieldName);

                        // Use the JdbcTypeHandler utility to set the value correctly
                        // based on the destination SQL type defined in metadata.
                        try {
                            // Parameter index is 1-based in JDBC
                            jdbcTypeHandler.writeField(ps, i + 1, meta, value);
                        } catch (SQLException e) {
                            // Log detailed error including field name, index, value type, and target SQL type
                            log.error("SQLException setting parameter for field '{}' (index {}) with value type {} to destType {}. Value: '{}'. SQL: [{}]. Error: {}",
                                    destFieldName, i + 1, (value != null ? value.getClass().getName() : "null"),
                                    meta.getDestSqlType(), value, sql, e.getMessage());
                            // Re-throw SQLException to allow Spring Batch skip/retry/fail logic to engage
                            throw e;
                        } catch (Exception e) {
                             // Catch other potential errors during type handling/setting
                             log.error("Unexpected error setting parameter for field '{}' (index {}). Value: '{}'. Error: {}",
                                     destFieldName, i + 1, value, e.getMessage(), e);
                             // Wrap in SQLException to ensure Batch fault tolerance can handle it
                             throw new SQLException("Failed setting parameter for " + destFieldName, e);
                        }
                    }
                })
                // .assertUpdates(true) // Optional: Verify that each statement in the batch affected rows (can fail on some DBs/configs)
                .build();
    }

     // Add methods here for other destination types like buildCsvFileWriter, buildApiWriter etc.
     /* Example for CSV Writer:
     private ItemWriter<Map<String, Object>> buildCsvFileWriter(Map<String, String> details, List<FieldMetadata> mappings, String writerName) {
        String filePath = details.get("filePath");
        // ... get delimiter, encoding, etc.
        Resource resource = new FileSystemResource(filePath);
        String[] fieldNames = mappings.stream().map(FieldMetadata::getDestFieldName).toArray(String[]::new);

        return new FlatFileItemWriterBuilder<Map<String, Object>>()
            .name(writerName)
            .resource(resource)
            // .encoding(...)
            .lineAggregator(new DelimitedLineAggregator<Map<String, Object>>() {{
                setDelimiter(","); // Use configured delimiter
                setFieldExtractor(new BeanWrapperFieldExtractor<Map<String, Object>>() {{
                    setNames(fieldNames); // Extract values for these fields (keys in the map)
                }});
            }})
            // .headerCallback(...) // Optional: Write header row
            .build();
     }
     */
}
