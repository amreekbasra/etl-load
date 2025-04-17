package com.example.etl.job;

import com.example.etl.model.EtlTaskConfig;
import com.example.etl.model.FieldMetadata;
import com.example.etl.model.SourceType;
import com.example.etl.service.DynamicJobService;
import com.example.etl.util.JdbcTypeHandler; // Import the type handler
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.RowMapper; // Import RowMapper
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet; // Import ResultSet
import java.sql.SQLException; // Import SQLException
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory responsible for creating step-scoped ItemReader beans
 * based on the dynamic EtlTaskConfig. This acts as the "Fetch Data" component.
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

    @Autowired // Inject the type handler utility bean
    private JdbcTypeHandler jdbcTypeHandler;

    @Value("${etl.default.fetch.size:500}") // Default JDBC fetch size from properties
    private int defaultFetchSize;

    /**
     * Defines the step-scoped bean for the dynamic ItemReader.
     * Spring Batch calls this method within a step's context to get the reader instance.
     *
     * @param configKey Injected from JobParameters via @Value - identifies the config for this run.
     * @return A configured ItemReader instance ready to fetch data in chunks.
     * @throws IllegalArgumentException if the configuration is invalid or source type unsupported.
     */
    @Bean(name = "dynamicItemReader")
    @Scope(value = "step", proxyMode = ScopedProxyMode.INTERFACES) // Proxy needed as it returns an interface
    public ItemReader<Map<String, Object>> createReader(
            @Value("#{jobParameters['configKey']}") String configKey // Inject from Job Params
    ) {
        log.info("Creating step-scoped ItemReader for configKey: {}", configKey);
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
                    // Pass mappings to buildJdbcReader
                    return buildJdbcReader(oracleDataSource, sourceDetails, config.getFieldMappings(), "oracleReader_" + configKey);
                case MSSQL_DB:
                    log.debug("Building MS SQL DB ItemReader for key: {}", configKey);
                    // Pass mappings to buildJdbcReader
                    return buildJdbcReader(msSqlDataSource, sourceDetails, config.getFieldMappings(), "mssqlReader_" + configKey);
                case FILE_CSV:
                    log.debug("Building CSV File ItemReader for key: {}", configKey);
                    return buildCsvFileReader(sourceDetails, config.getFieldMappings(), "csvReader_" + configKey);
                // Add cases for FILE_FIXED, FILE_JSON, API_REST etc.
                default:
                    log.error("Unsupported source type [{}] specified for reader in configKey: {}", sourceType, configKey);
                    throw new IllegalArgumentException("Unsupported source type for reader: " + sourceType);
            }
        } catch (Exception e) {
            log.error("Failed to create ItemReader for configKey [{}], SourceType [{}]: {}", configKey, sourceType, e.getMessage(), e);
            throw new RuntimeException("Failed to create ItemReader for " + sourceType, e);
        }
    }

    /**
     * Builds a configured JdbcCursorItemReader.
     * It uses fetchSize for chunked retrieval from the DB and a RowMapper
     * to convert each row into a Map.
     */
    private ItemReader<Map<String, Object>> buildJdbcReader(DataSource dataSource, Map<String, String> details, List<FieldMetadata> mappings, String readerName) {
        String tableName = details.get("tableName");
        String whereClause = details.getOrDefault("whereClause", "1=1");

        if (!StringUtils.hasText(tableName)) {
            throw new IllegalArgumentException("tableName is required in sourceDetails for DB reader.");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("fieldMappings are required for DB reader.");
        }

        // Construct the SELECT list based *only* on sourceFieldName from mappings
        String selectColumns = mappings.stream()
                .map(FieldMetadata::getSourceFieldName)
                .filter(StringUtils::hasText)
                .distinct()
                .collect(Collectors.joining(", "));

        if (!StringUtils.hasText(selectColumns)) {
             throw new IllegalArgumentException("No valid source field names found in fieldMappings for DB reader.");
        }

        String safeTableName = tableName.replaceAll("[^a-zA-Z0-9_.]", "");
        String sql = String.format("SELECT %s FROM %s WHERE %s", selectColumns, safeTableName, whereClause);

        log.info("Building JDBC Reader [{}] with SQL: [{}], Fetch Size: {}", readerName, sql, defaultFetchSize);

        // Use a custom RowMapper lambda for efficiency and direct use of JdbcTypeHandler
        RowMapper<Map<String, Object>> rowMapper = (rs, rowNum) -> {
            Map<String, Object> rowMap = new HashMap<>();
            // Iterate through the mappings provided for this specific ETL task
            for (FieldMetadata fieldMeta : mappings) {
                // Read the value using the source field name and type from metadata
                Object value = jdbcTypeHandler.readField(rs, fieldMeta);
                // Store the value in the map using the DESTINATION field name as the key.
                // This makes it ready for the ItemWriter which expects destination names.
                rowMap.put(fieldMeta.getDestFieldName(), value);
            }
            return rowMap;
        };

        return new JdbcCursorItemReaderBuilder<Map<String, Object>>()
                .name(readerName)
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(rowMapper) // Use our custom row mapper
                .fetchSize(defaultFetchSize) // Key for chunked fetching from DB
                .verifyCursorPosition(false)
                // Add .preparedStatementSetter() if WHERE clause needs parameters
                .build();
    }

    /**
     * Builds a configured FlatFileItemReader for CSV files.
     */
    private ItemReader<Map<String, Object>> buildCsvFileReader(Map<String, String> details, List<FieldMetadata> mappings, String readerName) {
        String filePath = details.get("filePath");
        boolean includesHeader = Boolean.parseBoolean(details.getOrDefault("includesHeader", "true"));
        String delimiter = details.getOrDefault("delimiter", ",");
        String encoding = details.getOrDefault("encoding", "UTF-8");

        if (!StringUtils.hasText(filePath)) {
            throw new IllegalArgumentException("filePath is required for FILE_CSV source");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("fieldMappings are required for FILE_CSV reader.");
        }

        Resource resource = new FileSystemResource(filePath);
        if (!resource.exists()) {
             throw new IllegalArgumentException("CSV file not found at path: " + filePath);
        }

        log.info("Building CSV Reader [{}] for file: {}, Delimiter: '{}', Header: {}", readerName, filePath, delimiter, includesHeader);

        // Field names expected in the file (order matters for tokenizer)
        String[] sourceFieldNames = mappings.stream()
                                     .map(FieldMetadata::getSourceFieldName)
                                     .toArray(String[]::new);

        return new FlatFileItemReaderBuilder<Map<String, Object>>()
                .name(readerName)
                .resource(resource)
                .encoding(encoding)
                .linesToSkip(includesHeader ? 1 : 0)
                .lineMapper(new DefaultLineMapper<Map<String, Object>>() {{
                    setLineTokenizer(new DelimitedLineTokenizer(delimiter) {{
                        setNames(sourceFieldNames);
                    }});
                    // Map FieldSet to Map<String, Object> using DESTINATION field names as keys
                    setFieldSetMapper(fieldSet -> {
                        Map<String, Object> rowMap = new HashMap<>();
                        for (FieldMetadata fieldMeta : mappings) {
                            String sourceName = fieldMeta.getSourceFieldName();
                            String destName = fieldMeta.getDestFieldName();
                            // Read as String, let writer/processor handle type conversion
                            String value = fieldSet.readString(sourceName);
                            rowMap.put(destName, value);
                        }
                        return rowMap;
                    });
                }})
                .build();
    }
}
