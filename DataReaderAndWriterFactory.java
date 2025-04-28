// --- core/factory/DataReaderFactory.java ---
package com.yourcompany.etl.core.factory;

import com.yourcompany.etl.core.reader.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Factory responsible for creating DataReader instances based on source type.
 * Uses Spring's ApplicationContext to get specific reader beans, which should be
 * defined with prototype scope.
 */
@Component
public class DataReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(DataReaderFactory.class);

    @Autowired
    private ApplicationContext context;

    /**
     * Creates a DataReader instance for the specified source type.
     * It looks for a Spring bean named "{sourceTypeInLowerCase}Reader"
     * (e.g., "oracledbReader", "mssqlReader", "flatfileReader").
     *
     * @param sourceType The type identifier (e.g., "ORACLE_DB", "MSSQL_DB", "FLAT_FILE"). Case-insensitive.
     * @return A new DataReader instance (prototype scope).
     * @throws IllegalArgumentException if sourceType is blank or no reader bean is found for the type.
     */
    public DataReader createReader(String sourceType) {
        if (!StringUtils.hasText(sourceType)) {
            throw new IllegalArgumentException("Source type cannot be null or empty.");
        }

        // Normalize the type and create the expected bean name
        // Example: "ORACLE_DB" -> "oracledbReader", "Mssql_db" -> "mssqldbReader"
        String beanName = sourceType.trim().toLowerCase().replace("_", "") + "Reader";
        log.debug("Attempting to find DataReader bean with name: {}", beanName);

        try {
            // Get the bean from the Spring context.
            // Crucially relies on the target bean being defined with @Scope("prototype").
            DataReader reader = context.getBean(beanName, DataReader.class);
            log.info("Successfully created DataReader instance for type '{}' using bean '{}'", sourceType, beanName);
            return reader;
        } catch (NoSuchBeanDefinitionException e) {
            log.error("No DataReader bean definition found with name '{}' for source type '{}'. " +
                      "Ensure a @Component(\"{}\") @Scope(\"prototype\") implementing DataReader exists.",
                      beanName, sourceType, beanName, e);
            throw new IllegalArgumentException("No DataReader configured for source type: " + sourceType);
        } catch (Exception e) {
            // Catch other potential errors during bean creation
            log.error("Failed to create DataReader bean '{}' for source type '{}'", beanName, sourceType, e);
            throw new RuntimeException("Error creating DataReader for source type: " + sourceType, e);
        }
    }
}

// --- core/factory/DataWriterFactory.java ---
package com.yourcompany.etl.core.factory;

import com.yourcompany.etl.core.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Factory responsible for creating DataWriter instances based on destination type.
 * Uses Spring's ApplicationContext to get specific writer beans, which should be
 * defined with prototype scope.
 */
@Component
public class DataWriterFactory {

    private static final Logger log = LoggerFactory.getLogger(DataWriterFactory.class);

    @Autowired
    private ApplicationContext context;

    /**
     * Creates a DataWriter instance for the specified destination type.
     * It looks for a Spring bean named "{destinationTypeInLowerCase}Writer"
     * (e.g., "oracledbWriter", "mssqlWriter", "flatfileWriter").
     *
     * @param destinationType The type identifier (e.g., "ORACLE_DB", "MSSQL_DB", "FLAT_FILE"). Case-insensitive.
     * @return A new DataWriter instance (prototype scope).
     * @throws IllegalArgumentException if destinationType is blank or no writer bean is found for the type.
     */
    public DataWriter createWriter(String destinationType) {
        if (!StringUtils.hasText(destinationType)) {
            throw new IllegalArgumentException("Destination type cannot be null or empty.");
        }

        // Normalize the type and create the expected bean name
        // Example: "ORACLE_DB" -> "oracledbWriter", "Flat_File" -> "flatfileWriter"
        String beanName = destinationType.trim().toLowerCase().replace("_", "") + "Writer";
        log.debug("Attempting to find DataWriter bean with name: {}", beanName);

        try {
            // Get the bean from the Spring context.
            // Relies on the target bean being defined with @Scope("prototype").
            DataWriter writer = context.getBean(beanName, DataWriter.class);
            log.info("Successfully created DataWriter instance for type '{}' using bean '{}'", destinationType, beanName);
            return writer;
        } catch (NoSuchBeanDefinitionException e) {
            log.error("No DataWriter bean definition found with name '{}' for destination type '{}'. " +
                      "Ensure a @Component(\"{}\") @Scope(\"prototype\") implementing DataWriter exists.",
                      beanName, destinationType, beanName, e);
            throw new IllegalArgumentException("No DataWriter configured for destination type: " + destinationType);
        } catch (Exception e) {
            // Catch other potential errors during bean creation
            log.error("Failed to create DataWriter bean '{}' for destination type '{}'", beanName, destinationType, e);
            throw new RuntimeException("Error creating DataWriter for destination type: " + destinationType, e);
        }
    }
}
