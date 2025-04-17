package com.example.etl.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configures the DataSources for Oracle (primary for Batch) and MS SQL Server.
 */
@Configuration
public class DataSourceConfig {

    // --- Oracle DataSource Configuration ---
    @Bean
    @Primary // Mark as primary, Spring Batch will use this by default
    @ConfigurationProperties("spring.datasource.oracle")
    public DataSourceProperties oracleDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "oracleDataSource")
    @Primary
    @ConfigurationProperties("spring.datasource.oracle.hikari") // Hikari specific props
    public HikariDataSource oracleDataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    // --- MS SQL DataSource Configuration ---
    @Bean
    @ConfigurationProperties("spring.datasource.mssql")
    public DataSourceProperties msSqlDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "msSqlDataSource")
    @ConfigurationProperties("spring.datasource.mssql.hikari")
    public HikariDataSource msSqlDataSource(@Qualifier("msSqlDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }
}
