package com.yourcompany.etl.core.processor;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Mapping;
import com.yourcompany.etl.core.model.Row;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat; // Caution: Not thread-safe if used concurrently
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Processes a source Row based on the JobConfig mappings.
 * Handles field renaming and type casting.
 * This is typically stateless and can be reused or created per job.
 */
public class MappingProcessor {

    private final List<Mapping> mappings;
    private final JobContext context;
    private final Logger log;
    private final JobConfig.ErrorHandling errorHandlingConfig;

    // Consider making date formatters configurable if needed
    // Be cautious with SimpleDateFormat - it's not thread-safe. Use DateTimeFormatter or create instances per call.
    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE;


    public MappingProcessor(JobContext context) {
        this.context = Objects.requireNonNull(context, "JobContext cannot be null");
        this.mappings = Objects.requireNonNull(context.getConfig().getMappings(), "Mappings cannot be null");
        this.log = Objects.requireNonNull(context.getLogger(), "Logger cannot be null");
        this.errorHandlingConfig = context.getConfig().getErrorHandling(); // Cache for quick access
    }

    /**
     * Transforms a source row into a destination row based on mappings.
     *
     * @param sourceRow The row read from the source (keys are source field names).
     * @return A new Row object with destination field names and casted types.
     * @throws EtlProcessingException if a non-recoverable error occurs during processing (e.g., null violation, uncastable type)
     * and the error strategy dictates failure.
     */
    public Row process(Row sourceRow) throws EtlProcessingException {
        Map<String, Object> destinationData = new HashMap<>();
        boolean rowHasError = false;

        for (Mapping mapping : mappings) {
            String sourceFieldName = mapping.getSourceFieldName();
            String destFieldName = mapping.getDestinationFieldName();
            Object sourceValue = sourceRow.getData().get(sourceFieldName);

            try {
                // 1. Handle Nullability
                if (sourceValue == null) {
                    if (!mapping.isDestNullable()) {
                        String errorMsg = String.format("Null value received for non-nullable destination field '%s' (source field '%s')",
                                                        destFieldName, sourceFieldName);
                        handleProcessingError(errorMsg, null, sourceRow); // Let handler decide fate
                        rowHasError = true; // Mark row as having an error
                        // Add null anyway or skip field? Adding null might cause DB errors later. Skipping might be safer.
                        // For now, we skip adding the field if it's a null violation handled by routing/logging.
                        continue; // Skip to next mapping for this row if error handled by routing/logging
                    }
                    destinationData.put(destFieldName, null);
                } else {
                    // 2. Handle Type Casting
                    Object castedValue = castType(sourceValue, mapping.getSourceFieldType(), mapping.getDestFieldType(), mapping);
                    destinationData.put(destFieldName, castedValue);
                }
            } catch (EtlProcessingException e) {
                // Catch errors specifically thrown by castType or null checks handled by FAIL_JOB strategy
                log.error("Unrecoverable processing error for source field '{}' -> dest field '{}'. Value: '{}'. Error: {}",
                          sourceFieldName, destFieldName, sourceValue, e.getMessage(), e);
                rowHasError = true;
                // Re-throw if the handler decided to fail the job
                throw e;
            } catch (Exception e) {
                // Catch unexpected exceptions during casting
                String errorMsg = String.format("Unexpected error casting source field '%s' to dest field '%s'. Value: '%s'. Type: %s -> %s",
                                                sourceFieldName, destFieldName, sourceValue, mapping.getSourceFieldType(), mapping.getDestFieldType());
                handleProcessingError(errorMsg, e, sourceRow);
                rowHasError = true;
                 // Skip to next mapping for this row if error handled by routing/logging
                continue;
            }
        }

        // If the row had an error handled by logging/routing, return null to signal it should be skipped/routed
        if (rowHasError && !"FAIL_JOB".equalsIgnoreCase(errorHandlingConfig.getStrategy())) {
            return null;
        }

        return new Row(destinationData);
    }

    /**
     * Centralized handling for processing errors based on job configuration.
     *
     * @param message   Description of the error.
     * @param exception The exception that occurred (can be null).
     * @param sourceRow The source row causing the error.
     * @throws EtlProcessingException if the strategy is FAIL_JOB.
     */
    private void handleProcessingError(String message, Throwable exception, Row sourceRow) throws EtlProcessingException {
        context.incrementAndGetRecordsFailed();
        log.error("Row Processing Error (Job {}): {}. Failed Records: {}", context.getJobId(), message, context.getRecordsFailed(), exception);

        // TODO: Implement ROUTE_TO_FILE or ROUTE_TO_TOPIC logic here
        // Example: errorWriter.writeError(sourceRow, message, exception);

        if ("FAIL_JOB".equalsIgnoreCase(errorHandlingConfig.getStrategy())) {
            // Fail immediately
            throw new EtlProcessingException(message, exception, sourceRow);
        } else if (errorHandlingConfig.getMaxErrorsAllowed() > 0 && context.getRecordsFailed() > errorHandlingConfig.getMaxErrorsAllowed()) {
            // Fail if max errors exceeded
            String failMsg = String.format("Maximum allowed processing errors (%d) exceeded for job %s.",
                                           errorHandlingConfig.getMaxErrorsAllowed(), context.getJobId());
            log.error(failMsg);
            throw new EtlProcessingException(failMsg, exception, sourceRow);
        }
        // Otherwise (LOG_ONLY or ROUTE strategy and within limits), the error is logged, and processing continues (row skipped).
    }


    /**
     * Attempts to cast the source value to the destination type.
     * Needs significant expansion for robust, production-ready type handling.
     *
     * @param value      The source value object.
     * @param sourceType The source type name (string, from config).
     * @param destType   The destination type name (string, from config).
     * @param mapping    The mapping definition (for context).
     * @return The casted value object.
     * @throws Exception if casting fails and cannot be recovered.
     */
    private Object castType(Object value, String sourceType, String destType, Mapping mapping) throws Exception {
        if (value == null) return null;

        // Normalize type names for comparison (case-insensitive, trim whitespace)
        String normDestType = destType.trim().toUpperCase();
        // String normSourceType = sourceType.trim().toUpperCase(); // Less critical usually

        try {
            switch (normDestType) {
                case "STRING":
                case "VARCHAR":
                case "NVARCHAR":
                case "TEXT":
                    return value.toString();

                case "LONG":
                case "BIGINT":
                    if (value instanceof Number) return ((Number) value).longValue();
                    if (value instanceof String) return Long.parseLong(((String) value).trim());
                    break; // Let default handling throw error

                case "INTEGER":
                case "INT":
                    if (value instanceof Number) return ((Number) value).intValue();
                    if (value instanceof String) return Integer.parseInt(((String) value).trim());
                    break;

                case "DOUBLE":
                case "FLOAT": // Treat float as double for simplicity here
                    if (value instanceof Number) return ((Number) value).doubleValue();
                    if (value instanceof String) return Double.parseDouble(((String) value).trim());
                    break;

                case "DECIMAL":
                case "NUMERIC":
                case "BIGDECIMAL":
                    if (value instanceof BigDecimal) return value;
                    if (value instanceof Number) return new BigDecimal(value.toString()); // Convert via string for precision
                    if (value instanceof String) return new BigDecimal(((String) value).trim());
                    break;

                case "BOOLEAN":
                case "BIT":
                     if (value instanceof Boolean) return value;
                     if (value instanceof Number) return ((Number) value).intValue() != 0; // 0 is false, others true
                     if (value instanceof String) {
                         String strVal = ((String) value).trim().toLowerCase();
                         return "true".equals(strVal) || "1".equals(strVal) || "y".equals(strVal) || "yes".equals(strVal);
                     }
                     break;

                case "TIMESTAMP":
                case "DATETIME":
                     if (value instanceof Timestamp) return value;
                     if (value instanceof LocalDateTime) return Timestamp.valueOf((LocalDateTime) value);
                     if (value instanceof ZonedDateTime) return Timestamp.from(((ZonedDateTime) value).toInstant());
                     if (value instanceof Instant) return Timestamp.from((Instant)value);
                     // Handle java.util.Date (careful with timezones)
                     if (value instanceof Date) return new Timestamp(((Date) value).getTime());
                     // Handle java.sql.Date (time part will be zero)
                      if (value instanceof java.sql.Date) return new Timestamp(((java.sql.Date) value).getTime());
                     if (value instanceof String) {
                         // Try common formats - make this configurable!
                         try { return Timestamp.valueOf(LocalDateTime.parse(((String) value).trim(), ISO_LOCAL_DATE_TIME)); } catch (DateTimeParseException ignored) {}
                         try { return Timestamp.valueOf(LocalDate.parse(((String) value).trim(), ISO_LOCAL_DATE).atStartOfDay()); } catch (DateTimeParseException ignored) {}
                         // Add SimpleDateFormat parsing as fallback (with caution)
                         // try { return new Timestamp(createSimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse((String) value).getTime()); } catch (ParseException ignored) {}
                     }
                     break;

                 case "DATE": // java.sql.Date (date part only)
                      if (value instanceof java.sql.Date) return value;
                      if (value instanceof LocalDate) return java.sql.Date.valueOf((LocalDate) value);
                      // Truncate time part from other date/time types
                      if (value instanceof Timestamp) return new java.sql.Date(((Timestamp) value).getTime());
                      if (value instanceof Date) return new java.sql.Date(((Date) value).getTime()); // Includes util.Date
                      if (value instanceof LocalDateTime) return java.sql.Date.valueOf(((LocalDateTime) value).toLocalDate());
                      if (value instanceof ZonedDateTime) return java.sql.Date.valueOf(((ZonedDateTime) value).toLocalDate());
                      if (value instanceof Instant) return java.sql.Date.valueOf(LocalDate.ofInstant((Instant)value, ZoneId.systemDefault())); // Use system default TZ? Configurable?
                      if (value instanceof String) {
                          try { return java.sql.Date.valueOf(LocalDate.parse(((String) value).trim(), ISO_LOCAL_DATE)); } catch (DateTimeParseException ignored) {}
                          // Add SimpleDateFormat parsing as fallback
                          // try { return new java.sql.Date(createSimpleDateFormat("yyyy-MM-dd").parse((String) value).getTime()); } catch (ParseException ignored) {}
                      }
                      break;

                 // Add cases for TIME, BLOB, CLOB, JSON, XML etc. as needed

                default:
                    log.warn("No specific casting logic found for destination type '{}'. Returning original value type {}.",
                             destType, value.getClass().getName());
                    return value; // Return original object if no specific cast found (may cause issues later)
            }
        } catch (NumberFormatException | DateTimeParseException | ClassCastException e) {
            // Catch common, expected parsing/casting errors
             throw new EtlProcessingException(String.format("Casting failed for value '%s' (%s) to type '%s'", value, value.getClass().getSimpleName(), destType), e, null);
        } catch (Exception e) {
             // Catch other unexpected errors during casting
             throw new EtlProcessingException(String.format("Unexpected error casting value '%s' (%s) to type '%s'", value, value.getClass().getSimpleName(), destType), e, null);
        }

        // If we fall through the switch without returning or throwing, it means conversion wasn't possible
        throw new EtlProcessingException(String.format("Cannot cast value '%s' (type %s, source type '%s') to destination type '%s'",
                                         value, value.getClass().getName(), sourceType, destType), null); // No specific exception cause
    }

    // Helper to create SimpleDateFormat instances if needed (remember thread-safety issues)
    // private SimpleDateFormat createSimpleDateFormat(String pattern) {
    //    return new SimpleDateFormat(pattern);
    // }
}
