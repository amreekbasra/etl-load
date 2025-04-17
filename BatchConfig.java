package com.example.etl.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Configuration;

/**
 * Basic Spring Batch configuration enabling batch features.
 * The JobRepository, JobLauncher, etc., are auto-configured
 * using the primary DataSource by default.
 */
@Configuration
@EnableBatchProcessing // Enables Spring Batch features
public class BatchConfig {
    // Auto-configuration handles most setup based on @EnableBatchProcessing
    // and the primary DataSource bean.
    // Customizations like a different DataSource for the Job Repository
    // would be configured here if needed.
}
