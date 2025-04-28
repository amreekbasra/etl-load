package com.yourcompany.etl.core.writer.impl;

import com.yourcompany.etl.core.JobContext;
import com.yourcompany.etl.core.exception.EtlProcessingException;
import com.yourcompany.etl.core.model.JobConfig;
import com.yourcompany.etl.core.model.Mapping;
import com.yourcompany.etl.core.model.Row;
import com.yourcompany.etl.core.writer.DataWriter;
import org.slf4j.Logger;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DataWriter implementation for writing data to a delimited flat file (e.g., CSV, TSV).
 * Handles dynamic header writing based on mappings and configurable delimiter/encoding.
 * Marked as Prototype scope.
 */
@Component("flatfileWriter") // Bean name matching factory logic
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CsvDataWriter implements DataWriter {

    private JobContext context;
    private Logger log;
    private BufferedWriter writer;
    private String delimiter;
    private boolean includeHeader;
    private List<String> headerColumns; // Order of columns in the output file
    private Charset charset;
    private Path filePath;

    @Override
    public void open(JobContext context) throws Exception {
        this.context = context;
        this.log = context.getLogger();
        JobConfig.DestinationConfig destConfig = context.getConfig().getDestination();
        Map<String, Object> connDetails = destConfig.getConnectionDetails();

        // Extract configuration
        String filePathStr = (String) connDetails.get("filePath");
        if (filePathStr == null || filePathStr.trim().isEmpty()) {
            throw new EtlProcessingException("Missing 'filePath' in destination connectionDetails for FlatFile writer.");
        }
        this.filePath = Paths.get(filePathStr);
        this.delimiter = (String) connDetails.getOrDefault("delimiter", ",");
        this.includeHeader = (Boolean) connDetails.getOrDefault("includeHeader", true);
        String encoding = (String) connDetails.getOrDefault("encoding", "UTF-8");
        try {
            this.charset = Charset.forName(encoding);
        } catch (Exception e) {
            log.warn("Invalid encoding '{}' specified, defaulting to UTF-8.", encoding);
            this.charset = StandardCharsets.UTF_8;
        }

        log.info("Opening CSV writer for file: {}, Delimiter: '{}', Encoding: {}, IncludeHeader: {}",
                 filePath, delimiter, charset.name(), includeHeader);

        // Ensure parent directories exist
        Path parentDir = filePath.getParent();
        if (parentDir != null) {
            Files.createDirectories(parentDir);
            log.debug("Ensured directory exists: {}", parentDir);
        }

        // Determine header order from destination field names in mappings
        this.headerColumns = context.getConfig().getMappings().stream()
                                .map(Mapping::getDestinationFieldName)
                                .collect(Collectors.toList());

        if (this.headerColumns.isEmpty()) {
             log.warn("No mappings found, header will be empty or based on first row's keys if includeHeader is true.");
             // Optionally decide to fail the job here if headers are strictly required
        }

        // Create BufferedWriter
        // Use FileOutputStream and OutputStreamWriter to specify encoding
        try {
             this.writer = new BufferedWriter(
                 new OutputStreamWriter(new FileOutputStream(filePath.toFile()), charset)
             );
             log.debug("BufferedWriter created for file: {}", filePath);
        } catch (IOException e) {
             log.error("Failed to create writer for file {}: {}", filePath, e.getMessage(), e);
             throw new EtlProcessingException("Failed to open file for writing: " + filePath, e);
        }


        // Write header row if configured
        if (includeHeader) {
            if (this.headerColumns.isEmpty()) {
                 log.warn("includeHeader is true, but no destination fields defined in mappings. Header will not be written.");
            } else {
                try {
                     writer.write(createDelimitedString(this.headerColumns));
                     writer.newLine();
                     log.debug("Header written successfully: {}", this.headerColumns);
                } catch (IOException e) {
                     log.error("Failed to write header to file {}: {}", filePath, e.getMessage(), e);
                     // Close writer and rethrow
                     try { close(); } catch (Exception ce) { log.error("Error closing writer after header write failure.", ce); }
                     throw new EtlProcessingException("Failed to write header to file: " + filePath, e);
                }
            }
        }
    }

    @Override
    public void writeBatch(List<Row> batch) throws Exception {
        log.debug("Writing batch of {} rows to {}", batch.size(), filePath);
        try {
            for (Row row : batch) {
                context.checkIfCancelled(); // Check for cancellation within the batch loop

                List<String> values = prepareRowValues(row);
                writer.write(createDelimitedString(values));
                writer.newLine();
            }
            // Flush the writer periodically or rely on close() for final flush
            // writer.flush(); // Flushing too often can impact performance
        } catch (IOException e) {
            log.error("Failed to write batch to file {}: {}", filePath, e.getMessage(), e);
            // This indicates a more severe IO issue, likely non-recoverable for this writer instance
            throw new EtlProcessingException("Failed to write batch to file: " + filePath, e);
        }
    }

    // Prepares the list of string values for a row, in the correct header order.
    private List<String> prepareRowValues(Row row) {
        Map<String, Object> data = row.getData();
        // If headerColumns is empty (no mappings), try using the keys from the first row? Risky.
        // Sticking to defined header order.
        return headerColumns.stream()
                .map(colName -> {
                    Object value = data.get(colName);
                    return formatValue(value); // Format object to string, handle nulls and quoting
                })
                .collect(Collectors.toList());
    }

    // Creates a single delimited string from a list of values.
    private String createDelimitedString(List<String> values) {
        return values.stream()
                .map(this::escapeValue) // Escape delimiter and quotes within values
                .collect(Collectors.joining(delimiter));
    }

    // Formats an object value to its string representation for the CSV.
    private String formatValue(Object value) {
        if (value == null) {
            return ""; // Represent null as empty string (configurable?)
        }
        // TODO: Add specific formatting for Dates, Timestamps, BigDecimals if needed
        // Example:
        // if (value instanceof Timestamp) {
        //    return someDateFormat.format((Timestamp) value);
        // }
        return value.toString();
    }

    // Basic CSV value escaping (handles quotes and delimiter). More robust libraries exist (e.g., Apache Commons CSV).
    private String escapeValue(String value) {
        if (value == null) {
            return "";
        }
        // If value contains delimiter, double quotes, or newline, enclose in double quotes
        if (value.contains(delimiter) || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            // Escape existing double quotes by doubling them
            String escaped = value.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }
        return value; // No escaping needed
    }


    @Override
    public void close() throws Exception {
        log.info("Closing CSV writer for file: {}", filePath);
        if (writer != null) {
            try {
                writer.flush(); // Ensure all buffered data is written
                log.debug("Writer flushed.");
            } catch (IOException e) {
                 log.error("Error flushing writer for file {}: {}", filePath, e.getMessage(), e);
                 // Still try to close
            } finally {
                 try {
                     writer.close();
                     log.debug("Writer closed successfully.");
                 } catch (IOException e) {
                     log.error("Error closing writer for file {}: {}", filePath, e.getMessage(), e);
                     // Throw exception on close failure as data might be lost/corrupted
                     throw new EtlProcessingException("Failed to close file writer: " + filePath, e);
                 } finally {
                      writer = null; // Help GC
                 }
            }
        } else {
            log.debug("Writer was already null or closed.");
        }
    }
}
