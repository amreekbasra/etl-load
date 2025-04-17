package com.example.etl.config;

import com.example.etl.util.JdbcTypeHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for utility beans used in the ETL process.
 */
@Configuration
public class EtlUtilConfig {

    /**
     * Provides a singleton bean for handling JDBC type conversions.
     * @return An instance of JdbcTypeHandler.
     */
    @Bean
    public JdbcTypeHandler jdbcTypeHandler() {
        return new JdbcTypeHandler();
    }
}
