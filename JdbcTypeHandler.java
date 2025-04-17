package com.example.etl.util;

import com.example.etl.model.FieldMetadata; // Assuming FieldMetadata is in this package or imported
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// No Spring stereotype needed if instantiated via @Bean in config
// import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate; // Example for modern date types
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Utility class to handle reading and writing values via JDBC
 * based on java.sql.Types information provided in FieldMetadata.
 *
 * This class is designed to be thread-safe and used as a singleton bean.
 */
public class JdbcTypeHandler {

    private static final Logger log = LoggerFactory.getLogger(JdbcTypeHandler.class);

    /**
     * Reads a value from the ResultSet for a specific field based on its metadata.
     *
     * @param rs        The ResultSet positioned at the current row.
     * @param fieldMeta Metadata defining the source field name and expected SQL type.
     * @return The value read from the ResultSet, or null if the value was SQL NULL.
     * @throws SQLException If a database access error occurs or the type is unsupported.
     */
    public Object readField(ResultSet rs, FieldMetadata fieldMeta) throws SQLException {
        String fieldName = fieldMeta.getSourceFieldName();
        int sqlType = fieldMeta.getSourceSqlType();
        Object value = null;

        try {
            // Use getObject as a robust default, but handle specific types for clarity/performance
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    value = rs.getString(fieldName);
                    break;

                case Types.BIT: // Often maps to boolean or number
                case Types.BOOLEAN:
                    value = rs.getBoolean(fieldName);
                    // Handle cases where BIT might be 0/1 - rs.getBoolean usually works
                    break;

                case Types.TINYINT:
                    value = rs.getByte(fieldName); // Or getShort if TINYINT maps higher
                    break;
                case Types.SMALLINT:
                    value = rs.getShort(fieldName);
                    break;
                case Types.INTEGER:
                    value = rs.getInt(fieldName);
                    break;
                case Types.BIGINT:
                    value = rs.getLong(fieldName);
                    break;

                case Types.REAL: // Typically maps to float
                    value = rs.getFloat(fieldName);
                    break;
                case Types.FLOAT: // Typically maps to double
                case Types.DOUBLE:
                    value = rs.getDouble(fieldName);
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    value = rs.getBigDecimal(fieldName);
                    break;

                case Types.DATE:
                    // Return java.sql.Date or potentially java.time.LocalDate if preferred
                    value = rs.getDate(fieldName);
                    // value = rs.getObject(fieldName, LocalDate.class); // Requires JDBC 4.2 driver
                    break;
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    // Return java.sql.Time or potentially java.time.LocalTime/OffsetTime
                    value = rs.getTime(fieldName);
                    // value = rs.getObject(fieldName, LocalTime.class); // JDBC 4.2
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                     // Return java.sql.Timestamp or potentially java.time.LocalDateTime/OffsetDateTime/Instant
                    value = rs.getTimestamp(fieldName);
                    // value = rs.getObject(fieldName, LocalDateTime.class); // JDBC 4.2
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    value = rs.getBytes(fieldName);
                    break;

                case Types.BLOB:
                    value = rs.getBlob(fieldName);
                    // WARNING: Reading large BLOBs fully into memory can cause OOM.
                    // Consider streaming if BLOBs are large.
                    break;
                case Types.CLOB:
                case Types.NCLOB:
                    value = rs.getClob(fieldName);
                    // WARNING: Reading large CLOBs fully into memory can cause OOM.
                    // Consider streaming if CLOBs are large.
                    break;

                // Add handling for other types as needed: ARRAY, STRUCT, REF, DATALINK, ROWID, SQLXML etc.
                case Types.ARRAY:
                    value = rs.getArray(fieldName);
                    break;
                case Types.STRUCT:
                     value = rs.getObject(fieldName); // Or getStruct if specific handling needed
                     break;
                case Types.REF:
                     value = rs.getRef(fieldName);
                     break;
                case Types.SQLXML:
                     value = rs.getSQLXML(fieldName);
                     break;

                default:
                    // Fallback to getObject for unknown or complex types
                    log.trace("Using getObject() for unhandled source SQL type ({}) for field '{}'", sqlType, fieldName);
                    value = rs.getObject(fieldName);
                    break;
            }

            // Crucial check after reading, as methods like getInt return 0 for SQL NULL
            if (rs.wasNull()) {
                return null;
            }
            return value;

        } catch (SQLException e) {
            log.error("SQLException reading field '{}' (expected SQL type {}): {}", fieldName, sqlType, e.getMessage());
            throw e; // Re-throw
        }
    }

    /**
     * Writes a value to the PreparedStatement at the specified index, handling type conversion
     * and nulls based on destination field metadata.
     *
     * @param ps        The PreparedStatement to update.
     * @param index     The 1-based index of the parameter to set.
     * @param fieldMeta Metadata defining the destination field name, SQL type, and nullability.
     * @param value     The value to write (should ideally be of a compatible Java type).
     * @throws SQLException If a database access error occurs or type conversion fails.
     */
    public void writeField(PreparedStatement ps, int index, FieldMetadata fieldMeta, Object value) throws SQLException {
        int destSqlType = fieldMeta.getDestSqlType();

        try {
            if (value == null) {
                if (!fieldMeta.isDestIsNullAllowed()) {
                    // Log a warning, but let the database potentially handle the constraint violation.
                    // Alternatively, throw an exception here if nulls are strictly disallowed by ETL logic.
                    log.warn("Attempting to write NULL to non-nullable destination field '{}' (index {}, type {})",
                            fieldMeta.getDestFieldName(), index, destSqlType);
                    // throw new SQLException("Cannot set NULL for non-nullable field: " + fieldMeta.getDestFieldName());
                }
                // Use setNull with the specific SQL type for portability
                ps.setNull(index, destSqlType);
                return;
            }

            // --- Handle potential type conversions before setting ---
            // This section might need expansion based on common source/destination mismatches
            // Example: Source reads String "123", Destination is Types.INTEGER
            if (value instanceof String && (destSqlType == Types.INTEGER || destSqlType == Types.BIGINT || destSqlType == Types.SMALLINT || destSqlType == Types.TINYINT)) {
                try {
                    value = Long.parseLong((String) value); // Convert string to number
                } catch (NumberFormatException nfe) {
                    log.error("Failed to parse String '{}' to Long for field '{}' (index {})", value, fieldMeta.getDestFieldName(), index);
                    throw new SQLException("Invalid number format for field " + fieldMeta.getDestFieldName(), nfe);
                }
            }
            // Add more conversions as needed (e.g., String to BigDecimal, String to Date/Timestamp using formatPattern)


            // --- Set the value using appropriate JDBC method or setObject ---
            // Using setObject(index, value, targetSqlType) is often preferred as it delegates
            // conversion logic to the JDBC driver, but sometimes specific setters are needed.

            switch (destSqlType) {
                // Explicit setters can sometimes be more reliable or performant
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    ps.setString(index, value.toString()); // Ensure it's a string
                    break;

                case Types.BIT:
                case Types.BOOLEAN:
                     if (value instanceof Boolean) {
                         ps.setBoolean(index, (Boolean) value);
                     } else if (value instanceof Number) {
                         // Handle numeric representations (e.g., 0/1)
                         ps.setBoolean(index, ((Number) value).intValue() != 0);
                     } else {
                         // Try to parse from String? ("true"/"false", "T"/"F", "0"/"1") - Add robust parsing if needed
                         ps.setBoolean(index, Boolean.parseBoolean(value.toString()));
                     }
                     break;

                case Types.TINYINT:
                     if (value instanceof Number) ps.setByte(index, ((Number) value).byteValue());
                     else ps.setByte(index, Byte.parseByte(value.toString()));
                     break;
                case Types.SMALLINT:
                     if (value instanceof Number) ps.setShort(index, ((Number) value).shortValue());
                     else ps.setShort(index, Short.parseShort(value.toString()));
                     break;
                case Types.INTEGER:
                     if (value instanceof Number) ps.setInt(index, ((Number) value).intValue());
                     else ps.setInt(index, Integer.parseInt(value.toString()));
                     break;
                case Types.BIGINT:
                     if (value instanceof Number) ps.setLong(index, ((Number) value).longValue());
                     else ps.setLong(index, Long.parseLong(value.toString()));
                     break;

                case Types.REAL:
                     if (value instanceof Number) ps.setFloat(index, ((Number) value).floatValue());
                     else ps.setFloat(index, Float.parseFloat(value.toString()));
                     break;
                case Types.FLOAT: // JDBC FLOAT often maps to double
                case Types.DOUBLE:
                     if (value instanceof Number) ps.setDouble(index, ((Number) value).doubleValue());
                     else ps.setDouble(index, Double.parseDouble(value.toString()));
                     break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                     if (value instanceof BigDecimal) ps.setBigDecimal(index, (BigDecimal) value);
                     else if (value instanceof Number) ps.setBigDecimal(index, BigDecimal.valueOf(((Number) value).doubleValue()));
                     else ps.setBigDecimal(index, new BigDecimal(value.toString()));
                     break;

                case Types.DATE:
                    if (value instanceof java.sql.Date) ps.setDate(index, (java.sql.Date) value);
                    else if (value instanceof java.util.Date) ps.setDate(index, new java.sql.Date(((java.util.Date) value).getTime()));
                    else if (value instanceof LocalDate) ps.setObject(index, value, Types.DATE); // JDBC 4.2+
                    else ps.setObject(index, value, Types.DATE); // Fallback conversion attempt
                    break;
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    if (value instanceof java.sql.Time) ps.setTime(index, (java.sql.Time) value);
                    else if (value instanceof java.util.Date) ps.setTime(index, new java.sql.Time(((java.util.Date) value).getTime()));
                    else if (value instanceof LocalTime) ps.setObject(index, value, Types.TIME); // JDBC 4.2+
                    else ps.setObject(index, value, Types.TIME);
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (value instanceof java.sql.Timestamp) ps.setTimestamp(index, (java.sql.Timestamp) value);
                    else if (value instanceof java.util.Date) ps.setTimestamp(index, new java.sql.Timestamp(((java.util.Date) value).getTime()));
                    else if (value instanceof LocalDateTime) ps.setObject(index, value, Types.TIMESTAMP); // JDBC 4.2+
                    else ps.setObject(index, value, Types.TIMESTAMP);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    if (value instanceof byte[]) ps.setBytes(index, (byte[]) value);
                    else ps.setObject(index, value, destSqlType); // Let driver attempt conversion
                    break;

                case Types.BLOB:
                    if (value instanceof Blob) ps.setBlob(index, (Blob) value);
                    // else if (value instanceof InputStream) ps.setBinaryStream(index, (InputStream) value); // For streaming
                    else if (value instanceof byte[]) ps.setBytes(index, (byte[]) value); // Smaller BLOBs as bytes
                    else ps.setObject(index, value, Types.BLOB);
                    break;
                case Types.CLOB:
                case Types.NCLOB:
                     if (value instanceof Clob) ps.setClob(index, (Clob) value);
                     // else if (value instanceof Reader) ps.setCharacterStream(index, (Reader) value); // For streaming
                     else if (value instanceof String) ps.setString(index, (String) value); // Smaller CLOBs as String
                     else ps.setObject(index, value, Types.CLOB);
                     break;

                // Add other types: ARRAY, STRUCT, REF, SQLXML etc. often require setObject or specific methods
                case Types.ARRAY:
                     if (value instanceof java.sql.Array) ps.setArray(index, (java.sql.Array) value);
                     else ps.setObject(index, value, Types.ARRAY);
                     break;
                 case Types.STRUCT:
                     if (value instanceof java.sql.Struct) ps.setObject(index, value, Types.STRUCT); // Use setObject for Struct
                     else ps.setObject(index, value, Types.STRUCT);
                     break;
                case Types.SQLXML:
                     if (value instanceof java.sql.SQLXML) ps.setSQLXML(index, (java.sql.SQLXML) value);
                     else ps.setObject(index, value, Types.SQLXML);
                     break;

                default:
                    // Fallback to setObject, providing the target SQL type hint
                    log.trace("Using setObject() for destination SQL type ({}) for field '{}' (index {})", destSqlType, fieldMeta.getDestFieldName(), index);
                    ps.setObject(index, value, destSqlType);
                    break;
            }

        } catch (Exception e) { // Catch broader exceptions during conversion/setting
            log.error("Error setting parameter for field '{}' (index {}) with value type {} to destType {}. Value: '{}'. Error: {}",
                    fieldMeta.getDestFieldName(), index, (value != null ? value.getClass().getName() : "null"),
                    destSqlType, value, e.getMessage(), e);
            // Wrap non-SQLExceptions in SQLException so Batch fault tolerance can handle them
            if (e instanceof SQLException) {
                throw (SQLException) e;
            } else {
                throw new SQLException("Type conversion/setting failed for field " + fieldMeta.getDestFieldName() + ": " + e.getMessage(), e);
            }
        }
    }
}
