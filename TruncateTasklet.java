package com.example.etl.job;

import com.example.etl.model.DestinationType;
import com.example.etl.model.EtlTaskConfig;
import com.example.etl.service.DynamicJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.Map;

/**
 * A Spring Batch Tasklet that truncates a target table based on the ETL configuration.
 * Must be step-scoped to access job/step context.
 */
@Component // Make it a Spring bean
@StepScope   // Crucial: Creates a new instance for each step execution
public class TruncateTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(TruncateTasklet.class);

    @Autowired
    private DynamicJobService dynamicJobService; // Service to retrieve the full task config

    // Inject available DataSources
    @Autowired(required = false) // Make optional if not all jobs use Oracle
    @Qualifier("oracleDataSource")
    private DataSource oracleDataSource;

    @Autowired(required = false) // Make optional if not all jobs use MSSQL
    @Qualifier("msSqlDataSource")
    private DataSource msSqlDataSource;

    // Inject the configKey from the step execution context (put there by StepContextParameterPutter)
    private final String configKey;

    // Constructor injection for step-scoped values
    public TruncateTasklet(@Value("#{stepExecutionContext['configKey']}") String configKey) {
        this.configKey = configKey;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Executing TruncateTasklet for configKey: {}", configKey);

        if (!StringUtils.hasText(configKey)) {
             log.error("ConfigKey is null or empty in TruncateTasklet. Cannot proceed.");
             // Fail the step immediately if configKey is missing
             throw new IllegalStateException("configKey not found in step execution context for TruncateTasklet.");
        }

        EtlTaskConfig config = dynamicJobService.getTaskConfig(configKey);
        Map<String, String> destDetails = config.getDestinationDetails();
        DestinationType destType = config.getDestinationType();
        String tableName = destDetails != null ? destDetails.get("tableName") : null;

        if (!StringUtils.hasText(tableName)) {
            log.error("Cannot truncate: destination 'tableName' is missing in config for key [{}].", configKey);
            throw new IllegalArgumentException("Destination table name missing for TRUNCATE step.");
        }

        DataSource targetDataSource;
        switch (destType) {
            case ORACLE_DB:
                if (oracleDataSource == null) throw new IllegalStateException("Oracle DataSource is not configured/available for TRUNCATE.");
                targetDataSource = oracleDataSource;
                break;
            case MSSQL_DB:
                 if (msSqlDataSource == null) throw new IllegalStateException("MS SQL DataSource is not configured/available for TRUNCATE.");
                targetDataSource = msSqlDataSource;
                break;
            default:
                 log.error("Cannot truncate: unsupported destination type [{}] for key [{}].", destType, configKey);
                 throw new IllegalArgumentException("Unsupported destination type for TRUNCATE: " + destType);
        }

        // Basic validation/sanitization of table name
        String safeTableName = tableName.replaceAll("[^a-zA-Z0-9_.]", "");
        if (!tableName.equals(safeTableName)) {
             log.warn("Potential unsafe characters detected in table name '{}', using sanitized '{}'. Review configuration.", tableName, safeTableName);
        }
        String sql = "TRUNCATE TABLE " + safeTableName;

        log.info("Attempting to execute truncate statement: [{}] on DSN type {}", sql, destType);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(targetDataSource);

        try {
             jdbcTemplate.execute(sql);
             log.info("Successfully truncated table: {}", safeTableName);
             // Optionally increment write count or add to summary
             contribution.incrementWriteCount(1); // Indicate one "write" operation (the truncate)
        } catch (Exception e) {
             log.error("Failed to truncate table {} (DSN type {}): {}", safeTableName, destType, e.getMessage(), e);
             // Let the exception propagate up to Spring Batch to fail the step
             throw e;
        }

        // Indicate that this tasklet is finished and the step should complete
        return RepeatStatus.FINISHED;
    }
}
