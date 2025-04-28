    package com.yourcompany.etl.service;

    import com.fasterxml.jackson.databind.ObjectMapper;
    import com.yourcompany.etl.core.model.JobConfig; // Your JobConfig model
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    import org.springframework.beans.factory.annotation.Autowired;
    import org.springframework.beans.factory.annotation.Qualifier;
    import org.springframework.dao.EmptyResultDataAccessException;
    import org.springframework.jdbc.core.JdbcTemplate;
    import org.springframework.stereotype.Service;
    import javax.sql.DataSource;

    @Service
    public class JobConfigurationService {

        private static final Logger log = LoggerFactory.getLogger(JobConfigurationService.class);

        private final JdbcTemplate jdbcTemplate;
        private final ObjectMapper objectMapper; // For parsing JSON stored in DB

        @Autowired
        public JobConfigurationService(
                // Inject the DataSource specifically configured for the config DB
                @Qualifier("configdbDataSource") DataSource configDataSource,
                ObjectMapper objectMapper) {
            this.jdbcTemplate = new JdbcTemplate(configDataSource);
            this.objectMapper = objectMapper;
            log.info("JobConfigurationService initialized with configdb DataSource.");
        }

        /**
         * Fetches the JobConfig JSON from the database based on an ID
         * and deserializes it into a JobConfig object.
         *
         * @param configId The unique ID of the job configuration.
         * @return The JobConfig object.
         * @throws RuntimeException if the config is not found or cannot be parsed.
         */
        public JobConfig getJobConfigById(String configId) {
            log.info("Fetching job configuration for ID: {}", configId);
            // Adjust SQL based on your actual table structure
            String sql = "SELECT config_json FROM etl_job_configurations WHERE config_id = ? AND is_active = 1"; // Example query

            try {
                String configJson = jdbcTemplate.queryForObject(sql, String.class, configId);

                if (configJson == null || configJson.trim().isEmpty()) {
                     log.error("Empty configuration JSON found for ID: {}", configId);
                    throw new RuntimeException("Empty configuration JSON found for ID: " + configId);
                }

                log.debug("Raw config JSON for ID {}: {}", configId, configJson); // Be careful logging sensitive data

                // Deserialize JSON string into JobConfig object
                JobConfig jobConfig = objectMapper.readValue(configJson, JobConfig.class);

                // Basic validation (ensure jobId matches or is set)
                if (jobConfig.getJobId() == null) {
                    log.warn("Job ID is null in the fetched configuration JSON for config ID {}. Setting it to config ID.", configId);
                    jobConfig.setJobId(configId); // Use configId as jobId if not present in JSON
                }

                log.info("Successfully fetched and parsed job configuration for ID: {}", configId);
                return jobConfig;

            } catch (EmptyResultDataAccessException e) {
                log.error("No active job configuration found for ID: {}", configId);
                throw new RuntimeException("No active job configuration found for ID: " + configId, e);
            } catch (Exception e) {
                log.error("Failed to fetch or parse job configuration for ID {}: {}", configId, e.getMessage(), e);
                throw new RuntimeException("Failed to fetch/parse job configuration for ID: " + configId, e);
            }
        }

        // --- Optional: Methods for fetching next job from a queue table ---
        /*
        public JobConfig getNextPendingJobAndUpdateStatus() {
            // 1. SELECT ... FROM etl_job_queue WHERE status = 'PENDING' ORDER BY priority, submit_time FOR UPDATE SKIP LOCKED
            // 2. If row found:
            //    a. Parse config_json or get config_id
            //    b. If config_id, call getJobConfigById(config_id)
            //    c. UPDATE etl_job_queue SET status = 'PROCESSING', start_time = NOW() WHERE job_queue_id = ?
            //    d. Commit transaction
            //    e. Return JobConfig
            // 3. If no row found, return null
            // Requires careful transaction management
            log.warn("getNextPendingJobAndUpdateStatus() not implemented.");
            return null;
        }
        */
    }

    // --- You also need to configure the DataSource bean itself ---
    // In a @Configuration class:
    /*
    import com.zaxxer.hikari.HikariDataSource;
    import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
    import org.springframework.boot.context.properties.ConfigurationProperties;
    import org.springframework.context.annotation.Bean;
    import org.springframework.context.annotation.Configuration;
    import javax.sql.DataSource;

    @Configuration
    public class DataSourceConfig {

        @Bean
        @ConfigurationProperties("spring.datasource.configdb") // Matches prefix in application.yml
        public DataSourceProperties configdbDataSourceProperties() {
            return new DataSourceProperties();
        }

        @Bean(name = "configdbDataSource") // The bean name used in @Qualifier
        @ConfigurationProperties("spring.datasource.configdb.hikari") // Optional Hikari specific props
        public DataSource configdbDataSource(DataSourceProperties configdbDataSourceProperties) {
            return configdbDataSourceProperties.initializeDataSourceBuilder()
                    .type(HikariDataSource.class) // Use HikariCP
                    .build();
        }

        // Define other DataSource beans for actual ETL source/destination if needed globally,
        // although often they are created dynamically by reader/writer based on job config.
    }
    */
    
