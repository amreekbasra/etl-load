// --- src/main/java/com/example/etl/model/EtlTaskConfig.java ---
package com.example.etl.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the configuration for a single ETL task, typically parsed from an AQ message.
 * Needs to be Serializable if passed via ExecutionContext or stored.
 */
public class EtlTaskConfig implements Serializable {
    private static final long serialVersionUID = 1L; // Good practice for Serializable

    private String taskId; // Unique ID for this specific ETL run request
    private SourceType sourceType;
    private DestinationType destinationType;
    // Details specific to the source (e.g., table name, file path, where clause)
    private Map<String, String> sourceDetails;
    // Details specific to the destination (e.g., table name)
    private Map<String, String> destinationDetails;
    // Ordered list of steps to perform for this task
    private List<EtlStep> steps;
    // Field mapping definitions (relevant for LOAD steps)
    private List<FieldMetadata> fieldMappings;
    // Optional: Notification details (e.g., email addresses, webhook URL)
    private Map<String, String> notificationDetails;

    // --- Getters and Setters ---
    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public SourceType getSourceType() { return sourceType; }
    public void setSourceType(SourceType sourceType) { this.sourceType = sourceType; }
    public DestinationType getDestinationType() { return destinationType; }
    public void setDestinationType(DestinationType destinationType) { this.destinationType = destinationType; }
    public Map<String, String> getSourceDetails() { return sourceDetails; }
    public void setSourceDetails(Map<String, String> sourceDetails) { this.sourceDetails = sourceDetails; }
    public Map<String, String> getDestinationDetails() { return destinationDetails; }
    public void setDestinationDetails(Map<String, String> destinationDetails) { this.destinationDetails = destinationDetails; }
    public List<EtlStep> getSteps() { return steps; }
    public void setSteps(List<EtlStep> steps) { this.steps = steps; }
    public List<FieldMetadata> getFieldMappings() { return fieldMappings; }
    public void setFieldMappings(List<FieldMetadata> fieldMappings) { this.fieldMappings = fieldMappings; }
    public Map<String, String> getNotificationDetails() { return notificationDetails; }
    public void setNotificationDetails(Map<String, String> notificationDetails) { this.notificationDetails = notificationDetails; }

    @Override
    public String toString() {
        return "EtlTaskConfig{" +
               "taskId='" + taskId + '\'' +
               ", sourceType=" + sourceType +
               ", destinationType=" + destinationType +
               ", steps=" + steps +
               // Avoid logging potentially large mappings/details in default toString
               ", sourceDetailsKeys=" + (sourceDetails != null ? sourceDetails.keySet() : "null") +
               ", destinationDetailsKeys=" + (destinationDetails != null ? destinationDetails.keySet() : "null") +
               ", fieldMappingCount=" + (fieldMappings != null ? fieldMappings.size() : 0) +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EtlTaskConfig that = (EtlTaskConfig) o;
        return Objects.equals(taskId, that.taskId); // Often identified by taskId
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId);
    }
}

// --- src/main/java/com/example/etl/model/FieldMetadata.java ---
package com.example.etl.model;

import java.io.Serializable;
import java.sql.Types;
import java.util.Objects;

/**
 * Defines mapping and type information for a single field in the ETL process.
 */
public class FieldMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private String sourceFieldName;     // Name in source (DB column, File header/position)
    private String destFieldName;       // Name in destination DB column
    // java.sql.Types constant for source (important for DB reads)
    private int sourceSqlType = Types.VARCHAR;
    // java.sql.Types constant for destination (important for DB writes)
    private int destSqlType = Types.VARCHAR;
    private boolean destIsNullAllowed = true;
    // Optional: For fixed-width files, indicates position/length (e.g., "1-10")
    private String fileColumnPosition;
    // Optional: Default value to use if source value is null or empty
    private String defaultValue;
    // Optional: Name of a simple transformation rule (e.g., "UPPERCASE", "TRIM")
    private String transformationRule;
    // Optional: Format string for date/number parsing/formatting
    private String formatPattern;

    // Default constructor needed for frameworks like Jackson
    public FieldMetadata() {}

    // Convenience constructor
    public FieldMetadata(String sourceFieldName, String destFieldName, int sourceSqlType, int destSqlType, boolean destIsNullAllowed) {
        this.sourceFieldName = sourceFieldName;
        this.destFieldName = destFieldName;
        this.sourceSqlType = sourceSqlType;
        this.destSqlType = destSqlType;
        this.destIsNullAllowed = destIsNullAllowed;
    }

    // --- Getters and Setters ---
    public String getSourceFieldName() { return sourceFieldName; }
    public void setSourceFieldName(String sourceFieldName) { this.sourceFieldName = sourceFieldName; }
    public String getDestFieldName() { return destFieldName; }
    public void setDestFieldName(String destFieldName) { this.destFieldName = destFieldName; }
    public int getSourceSqlType() { return sourceSqlType; }
    public void setSourceSqlType(int sourceSqlType) { this.sourceSqlType = sourceSqlType; }
    public int getDestSqlType() { return destSqlType; }
    public void setDestSqlType(int destSqlType) { this.destSqlType = destSqlType; }
    public boolean isDestIsNullAllowed() { return destIsNullAllowed; }
    public void setDestIsNullAllowed(boolean destIsNullAllowed) { this.destIsNullAllowed = destIsNullAllowed; }
    public String getFileColumnPosition() { return fileColumnPosition; }
    public void setFileColumnPosition(String fileColumnPosition) { this.fileColumnPosition = fileColumnPosition; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public String getTransformationRule() { return transformationRule; }
    public void setTransformationRule(String transformationRule) { this.transformationRule = transformationRule; }
    public String getFormatPattern() { return formatPattern; }
    public void setFormatPattern(String formatPattern) { this.formatPattern = formatPattern; }

    @Override
    public String toString() {
        return "FieldMetadata{" +
               "source='" + sourceFieldName + '\'' +
               ", dest='" + destFieldName + '\'' +
               ", destType=" + destSqlType +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMetadata that = (FieldMetadata) o;
        return Objects.equals(sourceFieldName, that.sourceFieldName) && Objects.equals(destFieldName, that.destFieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceFieldName, destFieldName);
    }
}

// --- src/main/java/com/example/etl/model/EtlStep.java ---
package com.example.etl.model;

/**
 * Enumeration of possible steps within an ETL task flow.
 */
public enum EtlStep {
    VALIDATE_SOURCE,        // Optional: Check source availability/schema
    TRUNCATE_DESTINATION,   // Truncate the target table before loading
    LOAD,                   // The main data loading step (read/process/write)
    VALIDATE_LOAD,          // Optional: Check row counts or data integrity post-load
    NOTIFY_SUCCESS,         // Send a notification on successful completion
    NOTIFY_FAILURE          // Send a notification on failure
    // Add other steps like ARCHIVE_SOURCE_FILE, UPDATE_STATUS_TABLE, etc.
}

// --- src/main/java/com/example/etl/model/SourceType.java ---
package com.example.etl.model;

/**
 * Enumeration of supported source types for ETL.
 */
public enum SourceType {
    ORACLE_DB,
    MSSQL_DB,
    FILE_CSV,       // Comma Separated Value file
    FILE_FIXED,     // Fixed Width file
    FILE_JSON,      // JSON file (potentially line-delimited)
    API_REST        // Data from a REST API endpoint
    // Add others like KAFKA, FTP, etc.
}

// --- src/main/java/com/example/etl/model/DestinationType.java ---
package com.example.etl.model;

/**
 * Enumeration of supported destination types for ETL.
 */
public enum DestinationType {
    ORACLE_DB,
    MSSQL_DB,
    FILE_CSV,
    API_REST // e.g., POSTing data to an endpoint
    // Add others
}

