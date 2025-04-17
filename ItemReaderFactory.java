package com.example.etl.job;

import com.example.etl.model.EtlTaskConfig;
import com.example.etl.model.FieldMetadata;
import com.example.etl.model.SourceType;
import com.example.etl.service.DynamicJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource; // Import Resource
import org.springframework.jdbc.core.ColumnMapRowMapper; // Simple row mapper
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils; // Import StringUtils

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory responsible for creating step-scoped ItemReader beans
 * based on the dynamic EtlTaskConfig.
 */
@Component // Factory itself is a singleton managed by Spring
public class ItemReaderFactory {
    private static final Logger log = LoggerFactory.getLogger(ItemReaderFactory.class);

    @Autowired
    private DynamicJobService dynamicJobService; // Service to retrieve the full task config

    // Inject available DataSources
    @Autowired
    @Qualifier("oracleDataSource")
    private DataSource oracleDataSource;

    @Autowired
    @Qualifier("msSqlDataSource")
    private DataSource msSqlDataSource;

    @Value("${etl.default.fetch.size:500}") // Default JDBC fetch size from properties
    private int defaultFetchSize;

    /**
     * Defines the step-scoped bean for the dynamic ItemReader.
     * Spring Batch will call this method within the context of a specific step execution
     * when the 'dynamicItemReader' bean is requested.
     *
     * @param configKey Injected from JobParameters via @Value - identifies the config for this run.
     * @return A configured ItemReader instance for the specific step execution.
     * @throws IllegalArgumentException if the configuration is invalid or source type unsupported.
     */
    @Bean(name = "dynamicItemReader")
    @Scope(value = "step", proxyMode = ScopedProxyMode.INTERFACES) // Proxy needed as it returns an interface
    public ItemReader<Map<String, Object>> createReader(
            @Value("#{jobParameters['configKey']}") String configKey // Inject from Job Params
    ) {
        log.info("Creating step-scoped ItemReader for configKey: {}", configKey);
        // Retrieve the full configuration using the key
        EtlTaskConfig config = dynamicJobService.getTaskConfig(configKey);
        Map<String, String> sourceDetails = config.getSourceDetails();
        SourceType sourceType = config.getSourceType();

        if (sourceDetails == null) {
             throw new IllegalArgumentException("Source details are missing in EtlTaskConfig for key: " + configKey);
        }

        try {
            switch (sourceType) {
                case ORACLE_DB:
                    log.debug("Building Oracle DB ItemReader for key: {}", configKey);
                    return buildJdbcReader(oracleDataSource, sourceDetails, config.getFieldMappings(), "oracleReader_" + configKey);
                case MSSQL_DB:
                    log.debug("Building MS SQL DB ItemReader for key: {}", configKey);
                    return buildJdbcReader(msSqlDataSource, sourceDetails, config.getFieldMappings(), "mssqlReader_" + configKey);
                case FILE_CSV:
                    log.debug("Building CSV File ItemReader for key: {}", configKey);
                    return buildCsvFileReader(sourceDetails, config.getFieldMappings(), "csvReader_" + configKey);
                // Add cases for FILE_FIXED, FILE_JSON, API_REST etc.
                // case FILE_FIXED:
                //    return buildFixedWidthFileReader(...)
                default:
                    log.error("Unsupported source type [{}] specified for reader in configKey: {}", sourceType, configKey);
                    throw new IllegalArgumentException("Unsupported source type for reader: " + sourceType);
            }
        } catch (Exception e) {
            log.error("Failed to create ItemReader for configKey [{}], SourceType [{}]: {}", configKey, sourceType, e.getMessage(), e);
            // Propagate exception to fail the step initialization
            throw new RuntimeException("Failed to create ItemReader for " + sourceType, e);
        }
    }

    /**
     * Builds a configured JdbcCursorItemReader.
     *
     * @param dataSource The DataSource to connect to.
     * @param details    Map containing source details like "tableName", "whereClause".
     * @param mappings   List of FieldMetadata to determine which columns to select.
     * @param readerName A unique name for the reader bean.
     * @return A configured JdbcCursorItemReader.
     */
    private ItemReader<Map<String, Object>> buildJdbcReader(DataSource dataSource, Map<String, String> details, List<FieldMetadata> mappings, String readerName) {
        String tableName = details.get("tableName");
        // Provide a default WHERE clause if none is specified
        String whereClause = details.getOrDefault("whereClause", "1=1");

        if (!StringUtils.hasText(tableName)) {
            throw new IllegalArgumentException("tableName is required in sourceDetails for DB reader.");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("fieldMappings are required for DB reader to know which columns to select.");
        }

        // Construct the SELECT statement safely using only specified source field names
        String selectColumns = mappings.stream()
                .map(FieldMetadata::getSourceFieldName)
                .filter(StringUtils::hasText) // Ensure column names are not empty
                .distinct() // Avoid selecting the same column twice if mapped multiple times
                .collect(Collectors.joining(", "));

        if (!StringUtils.hasText(selectColumns)) {
             throw new IllegalArgumentException("No valid source field names found in fieldMappings for DB reader.");
        }

        // Basic validation/sanitization of table name (prevent simple injection)
        // For production, use a stricter allow-list or schema validation.
        String safeTableName = tableName.replaceAll("[^a-zA-Z0-9_.]", "");
        // WHERE clause might contain complex logic, handle with care or use parameters if possible
        String sql = String.format("SELECT %s FROM %s WHERE %s", selectColumns, safeTableName, whereClause);

        log.info("Building JDBC Reader [{}] with SQL: {}", readerName, sql);

        return new JdbcCursorItemReaderBuilder<Map<String, Object>>()
                .name(readerName) // Unique name for the reader bean instance
                .dataSource(dataSource)
                .sql(sql)
                // ColumnMapRowMapper conveniently maps each row to a Map<String, Object>
                // Keys are column names (as returned by JDBC, case may vary by DB/driver)
                .rowMapper(new ColumnMapRowMapper())
                .fetchSize(defaultFetchSize) // Set fetch size for performance/memory management
                // Add .preparedStatementSetter() if your WHERE clause uses '?' placeholders
                .verifyCursorPosition(false) // Often needed for Oracle LOBs or complex queries
                .build();
    }

    /**
     * Builds a configured FlatFileItemReader for CSV files.
     *
     * @param details    Map containing source details like "filePath", "delimiter", "includesHeader".
     * @param mappings   List of FieldMetadata defining the expected columns/source field names.
     * @param readerName A unique name for the reader bean.
     * @return A configured FlatFileItemReader.
     */
    private ItemReader<Map<String, Object>> buildCsvFileReader(Map<String, String> details, List<FieldMetadata> mappings, String readerName) {
        String filePath = details.get("filePath");
        boolean includesHeader = Boolean.parseBoolean(details.getOrDefault("includesHeader", "true"));
        String delimiter = details.getOrDefault("delimiter", ",");
        String encoding = details.getOrDefault("encoding", "UTF-8"); // Default encoding

        if (!StringUtils.hasText(filePath)) {
            throw new IllegalArgumentException("filePath is required in sourceDetails for FILE_CSV reader.");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("fieldMappings are required for FILE_CSV reader to map columns.");
        }

        Resource resource = new FileSystemResource(filePath);
        if (!resource.exists()) {
             throw new IllegalArgumentException("CSV file not found at path: " + filePath);
        }

        log.info("Building CSV Reader [{}] for file: {}, Delimiter: '{}', Header: {}", readerName, filePath, delimiter, includesHeader);

        // Extract source field names from metadata; these will be the keys in the output map.
        // The order matters for the tokenizer.
        String[] fieldNames = mappings.stream()
                                     .map(FieldMetadata::getSourceFieldName)
                                     .toArray(String[]::new);

        if (fieldNames.length == 0) {
             throw new IllegalArgumentException("No source field names defined in mappings for CSV reader.");
        }

        return new FlatFileItemReaderBuilder<Map<String, Object>>()
                .name(readerName) // Unique name for the reader bean instance
                .resource(resource)
                .encoding(encoding)
                .linesToSkip(includesHeader ? 1 : 0) // Skip the header row if specified
                .lineMapper(new DefaultLineMapper<Map<String, Object>>() {{
                    // Tokenizer splits the line into fields
                    setLineTokenizer(new DelimitedLineTokenizer(delimiter) {{
                        setNames(fieldNames); // Assign names to tokenized fields based on metadata order
                        // setStrict(false); // Set to false if lines might have fewer fields than expected
                    }});
                    // FieldSetMapper maps the tokenized fields (FieldSet) to our target Map object
                    setFieldSetMapper(fieldSet -> {
                        Map<String, Object> rowMap = new HashMap<>();
                        for (String fieldName : fieldSet.getNames()) {
                            // Read all fields as String initially. Type conversion can happen
                            // in an ItemProcessor or before writing if needed.
                            // Use readString which handles quotes; avoid readRawString unless necessary.
                            rowMap.put(fieldName, fieldSet.readString(fieldName));
                        }
                        return rowMap;
                    });
                }})
                .build();
    }

     // Add methods here for other source types like buildFixedWidthFileReader, buildJsonFileReader, buildApiReader etc.
}
