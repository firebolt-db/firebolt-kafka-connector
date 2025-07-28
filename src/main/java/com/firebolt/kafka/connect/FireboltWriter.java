package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverterFactory;
import com.firebolt.kafka.connect.service.FireboltDbService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles writing records to Firebolt database.
 * Supports batching and automatic table creation/evolution.
 */
@Slf4j
public class FireboltWriter {

    private final SinkConfig config;
    private final Connection connection;
    private final List<FireboltRecord> recordBatch;
    private final Map<String, PreparedStatement> insertStatements;
    private final Map<String, TableSchema> tableSchemas;
    private final FireboltDbService fireboltDbService;
    private long lastFlushTime;

    public FireboltWriter(SinkConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
        this.recordBatch = new ArrayList<>();
        this.insertStatements = new ConcurrentHashMap<>();
        this.tableSchemas = new ConcurrentHashMap<>();
        this.fireboltDbService = new FireboltDbService();
        this.lastFlushTime = System.currentTimeMillis();
    }

    /**
     * Writes a record to the batch. Will flush if batch size or timeout is reached.
     *
     * @param record The record to write
     */
    public void write(FireboltRecord record, TableSchema tableSchema) {
        synchronized (recordBatch) {
            tableSchemas.put(tableSchema.getTableName(), tableSchema);
            recordBatch.add(record);

            // Check if we should flush
            if (shouldFlush()) {
                flush();
            }
        }
    }

    /**
     * Flushes all batched records to Firebolt.
     */
    public void flush() {
        synchronized (recordBatch) {
            if (recordBatch.isEmpty()) {
                return;
            }

            log.info("Flushing {} records to Firebolt", recordBatch.size());

            try {
                // Group records by table
                Map<String, List<FireboltRecord>> recordsByTable = groupRecordsByTable(recordBatch);

                // Write records for each table
                for (Map.Entry<String, List<FireboltRecord>> entry : recordsByTable.entrySet()) {
                    String tableName = entry.getKey();
                    List<FireboltRecord> records = entry.getValue();

                    writeRecordsToTable(tableName, records);
                }

                // Clear the batch
                recordBatch.clear();
                lastFlushTime = System.currentTimeMillis();

                log.info("Successfully flushed records to Firebolt");

            } catch (Exception e) {
                log.error("Error flushing records to Firebolt", e);
                throw new RuntimeException("Error flushing records", e);
            }
        }
    }

    /**
     * Closes the writer and releases resources.
     */
    public void close() {
        try {
            // Flush any remaining records
            flush();

            // Close prepared statements
            for (PreparedStatement stmt : insertStatements.values()) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.warn("Error closing prepared statement", e);
                }
            }
            insertStatements.clear();

            log.info("FireboltWriter closed");

        } catch (Exception e) {
            log.error("Error closing FireboltWriter", e);
        }
    }

    /**
     * Clears the cached table schemas. Useful for testing when table structure changes.
     */
    public void clearTableSchemaCache() {
        tableSchemas.clear();
        insertStatements.clear();
        log.info("Cleared table schema cache and prepared statements");
    }

    private boolean shouldFlush() {
        // Implement proper batching logic:
        // 1. Flush when batch reaches a reasonable size (e.g., 100 records)
        // 2. Flush when batch has been sitting for too long (e.g., 30 seconds)

        final int BATCH_SIZE_THRESHOLD = 100;
        final long BATCH_TIME_THRESHOLD_MS = 30000; // 30 seconds

        synchronized (recordBatch) {
            if (recordBatch.isEmpty()) {
                return false;
            }

            // Flush if batch size threshold reached
            if (recordBatch.size() >= BATCH_SIZE_THRESHOLD) {
                log.debug("Flushing due to batch size threshold: {} records", recordBatch.size());
                return true;
            }

            // Flush if batch has been sitting too long
            long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;
            if (timeSinceLastFlush >= BATCH_TIME_THRESHOLD_MS) {
                log.debug("Flushing due to time threshold: {} ms since last flush", timeSinceLastFlush);
                return true;
            }

            return false;
        }
    }

    private Map<String, List<FireboltRecord>> groupRecordsByTable(List<FireboltRecord> records) {
        Map<String, List<FireboltRecord>> recordsByTable = new ConcurrentHashMap<>();

        for (FireboltRecord record : records) {
            recordsByTable.computeIfAbsent(record.getTableName(), k -> new ArrayList<>()).add(record);
        }

        return recordsByTable;
    }

    private void writeRecordsToTable(String tableName, List<FireboltRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        log.debug("Writing {} records to table {}", records.size(), tableName);

        TableSchema tableSchema = tableSchemas.get(tableName);

        // since no schema evolution is supported, keep track of all the column names that are present in the records and filter out the ones that are not part of the table schema column names
        Set<String> columnNamesFromRecords = computeColumnNamesFromRecords(records);
        Set<String> validColumnNames = filterValidColumns(columnNamesFromRecords, tableSchema);

        // Get or create prepared statement
        PreparedStatement stmt = getOrCreateInsertStatement(tableName, tableSchema, validColumnNames);

        // Execute batch insert
        for (FireboltRecord record : records) {
            setStatementParameters(stmt, record, tableSchema, validColumnNames);
            stmt.addBatch();
        }

        int[] results = stmt.executeBatch();
        log.debug("Batch insert results: {} rows affected", results.length);

        // Clear batch
        stmt.clearBatch();
        // once clear batch on firebolt prepared statements are implemented properly then we can remove this and only keep clearBatch()
        stmt.clearParameters();
    }

    private PreparedStatement getOrCreateInsertStatement(String tableName, TableSchema schema, Set<String> validColumnNames) throws SQLException {
        String statementKey = tableName + "_" + String.join(",", validColumnNames);
        return insertStatements.computeIfAbsent(statementKey, k -> {
            try {
                String insertSQL = buildInsertSQL(tableName, schema, validColumnNames);
                log.debug("Prepared insert SQL: {}", insertSQL);
                return connection.prepareStatement(insertSQL);
            } catch (SQLException e) {
                throw new RuntimeException("Error creating insert statement for " + tableName, e);
            }
        });
    }

    private String buildInsertSQL(String tableName, TableSchema schema, Set<String> validColumnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");

        List<String> columnNames = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();

        for (TableSchema.Column column : schema.getColumns()) {
            if (validColumnNames.contains(column.getName())) {
                // Quote column names to handle case-sensitive columns
                String columnName = "\"" + column.getName() + "\"";
                columnNames.add(columnName);
                placeholders.add("?");
            }
        }

        sql.append(String.join(", ", columnNames));
        sql.append(") VALUES (");
        sql.append(String.join(", ", placeholders));
        sql.append(")");

        return sql.toString();
    }

    private void setStatementParameters(PreparedStatement stmt, FireboltRecord record, TableSchema schema, Set<String> validColumnNames) throws SQLException {
        Map<String, KafkaMessageColumnValue> columnValues = record.getColumnValues();

        // DEBUG: Log parameter setting start
        log.info("DEBUG: Setting statement parameters for record with column values: {}", columnValues);
        log.error("DEBUG: setStatementParameters called with {} columns", schema.getColumns().size());
        log.error("DEBUG: setStatementParameters - processing {} valid columns", validColumnNames.size());

        int parameterIndex = 1;
        for (TableSchema.Column column : schema.getColumns()) {
            if (validColumnNames.contains(column.getName())) {
                String columnName = column.getName();
                KafkaMessageColumnValue value = columnValues.get(columnName);
                log.error("DEBUG: Processing column '{}' with dataType '{}' and value: {}", columnName, column.getDataType(), value != null ? value.getValue() : "null");

                if (value == null || value.getValue() == null) {
                    stmt.setNull(parameterIndex, column.getSqlType());
                } else {
                    ColumnDataTypeConverter columnDataTypeConverter = ColumnDataTypeConverterFactory.getInstance().getConverter(column);
                    columnDataTypeConverter.convertAndSet(stmt, parameterIndex, value, column);
                }

                parameterIndex++;
            }
        }

        // DEBUG: Log total parameters set
        log.info("DEBUG: Set {} parameters for statement", parameterIndex - 1);
    }
//    private void setStatementParameters(PreparedStatement stmt, FireboltRecord record, TableSchema schema, Set<String> validColumnNames) throws SQLException {
//        Map<String, KafkaValue> columnValues = record.getColumnValues();
//
//        // DEBUG: Log parameter setting start
//        log.info("DEBUG: Setting statement parameters for record with column values: {}", columnValues);
//        log.error("DEBUG: setStatementParameters called with {} columns", schema.getColumns().size());
//        log.error("DEBUG: setStatementParameters - processing {} valid columns", validColumnNames.size());
//
//        int parameterIndex = 1;
//        for (TableSchema.Column column : schema.getColumns()) {
//            if (validColumnNames.contains(column.getName())) {
//                String columnName = column.getName();
//                KafkaValue value = columnValues.get(columnName);
//                log.error("DEBUG: Processing column '{}' with dataType '{}' and value: {}", columnName, column.getDataType(), value.getValue());
//
//                if (value.getValue() == null) {
//                    stmt.setNull(parameterIndex, column.getSqlType());
//                } else if (isArrayColumn(column)) {
//                    // Handle ARRAY columns first (before timestamp checking)
//                    if (value instanceof String) {
//                        String arrayJsonString = (String) value;
//                        log.info("DEBUG: Setting parameter {} for column '{}' (type: {}) with array JSON string: '{}'",
//                                parameterIndex, columnName, column.getDataType(), arrayJsonString);
//
//                        // Use setArray for numeric/integer arrays, setString for others
//                        if (isDateArrayColumn(column)) {
//                            // Handle DATE arrays - convert integer elements to java.sql.Date
//                            log.info("DEBUG: Processing DATE array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//                            java.sql.Date[] dateArray = parseJsonArrayToDateArray(arrayJsonString);
//                            log.info("DEBUG: Parsed JSON array to Date[]: {}", java.util.Arrays.toString(dateArray));
//
//                            // Create SQL Array from Date[]
//                            java.sql.Array sqlArray = connection.createArrayOf("DATE", dateArray);
//                            stmt.setArray(parameterIndex, sqlArray);
//                            log.info("DEBUG: ✅ Successfully set DATE array parameter {} with setArray using Date[]: {}",
//                                    parameterIndex, java.util.Arrays.toString(dateArray));
//                        } else if (isTimestampArrayColumn(column)) {
//                            // Handle TIMESTAMP arrays - convert long elements to java.sql.Timestamp
//                            log.info("DEBUG: Processing TIMESTAMP array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//                            java.sql.Timestamp[] timestampArray = parseJsonArrayToTimestampArray(arrayJsonString);
//                            log.info("DEBUG: Parsed JSON array to Timestamp[]: {}", java.util.Arrays.toString(timestampArray));
//
//                            // Create SQL Array from Timestamp[]
//                            java.sql.Array sqlArray = connection.createArrayOf("TIMESTAMP", timestampArray);
//                            stmt.setArray(parameterIndex, sqlArray);
//                            log.info("DEBUG: ✅ Successfully set TIMESTAMP array parameter {} with setArray using Timestamp[]: {}",
//                                    parameterIndex, java.util.Arrays.toString(timestampArray));
//                        } else if (isTimestamptzArrayColumn(column)) {
//                            // Handle TIMESTAMPTZ arrays - convert long elements to java.sql.Timestamp
//                            log.info("DEBUG: Processing TIMESTAMPTZ array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//                            java.sql.Timestamp[] timestamptzArray = parseJsonArrayToTimestamptzArray(arrayJsonString);
//                            log.info("DEBUG: Parsed JSON array to Timestamp[]: {}", java.util.Arrays.toString(timestamptzArray));
//
//                            // Create SQL Array from Timestamp[]
//                            java.sql.Array sqlArray = connection.createArrayOf("TIMESTAMPTZ", timestamptzArray);
//                            stmt.setArray(parameterIndex, sqlArray);
//                            log.info("DEBUG: ✅ Successfully set TIMESTAMPTZ array parameter {} with setArray using Timestamp[]: {}",
//                                    parameterIndex, java.util.Arrays.toString(timestamptzArray));
//                        } else if (isNestedArrayColumn(column)) {
//                            // Handle nested arrays (arrays of arrays) using setString with JSON format
//                            // Firebolt JDBC doesn't support nested arrays via createArrayOf, so we use JSON format
//                            log.info("DEBUG: Processing nested array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//                            log.error("DEBUG: Processing nested array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//
//                            // Check if the JSON contains extreme values that might cause parsing issues
//                            if (arrayJsonString.contains("-2147483648") || arrayJsonString.contains("2147483647")) {
//                                log.warn("DEBUG: JSON contains extreme integer values that may cause parsing issues: {}", arrayJsonString);
//                            }
//
//                            // For nested arrays, we need to use setString with JSON format
//                            // The Firebolt JDBC driver will parse the JSON format correctly
//                            stmt.setString(parameterIndex, arrayJsonString);
//                            log.info("DEBUG: Successfully set nested array parameter {} with setString using JSON: '{}'",
//                                    parameterIndex, arrayJsonString);
//                        } else if (isIntegerArrayColumn(column)) {
//                            // Handle INTEGER arrays using setArray with Integer type
//                            log.info("DEBUG: Processing INTEGER array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//                            log.error("DEBUG: Processing INTEGER array column '{}' with JSON: '{}'", columnName, arrayJsonString);
//
//                            // Parse JSON array to Integer[] and use setArray
//                            Integer[] integerArray = parseJsonArrayToIntegerArray(arrayJsonString);
//                            log.info("DEBUG: Parsed JSON array to Integer[]: {}", java.util.Arrays.toString(integerArray));
//
//                            // Create SQL Array from Integer[]
//                            java.sql.Array sqlArray = connection.createArrayOf("INTEGER", integerArray);
//                            stmt.setArray(parameterIndex, sqlArray);
//                            log.info("DEBUG: Successfully set INTEGER array parameter {} with setArray using Integer[]: {}",
//                                    parameterIndex, java.util.Arrays.toString(integerArray));
//                        } else if (isNumericArrayColumn(column)) {
//                            // Parse JSON array to appropriate array type and use setArray
//                            String dataType = column.getDataType().toLowerCase();
//                            log.info("DEBUG: Checking array type for column '{}', dataType: '{}', contains bigint: {}, contains int: {}",
//                                    columnName, dataType, dataType.contains("bigint"), dataType.contains("int"));
//                            log.error("DEBUG: Processing numeric array column '{}' with dataType '{}'", columnName, dataType);
//
//                            if (dataType.contains("bigint")) {
//                                // Handle BIGINT arrays
//                                Long[] longArray = parseJsonArrayToLongArray(arrayJsonString);
//                                log.info("DEBUG: Parsed JSON array to Long[]: {}", java.util.Arrays.toString(longArray));
//
//                                // Create SQL Array from Long[]
//                                java.sql.Array sqlArray = connection.createArrayOf("BIGINT", longArray);
//                                stmt.setArray(parameterIndex, sqlArray);
//                                log.info("DEBUG: Successfully set BIGINT array parameter {} with setArray using Long[]: {}",
//                                        parameterIndex, java.util.Arrays.toString(longArray));
//
//                            } else if (dataType.contains("real") || dataType.contains("float")) {
//                                // Handle REAL/FLOAT arrays
//                                Float[] floatArray = parseJsonArrayToFloatArray(arrayJsonString);
//                                log.info("DEBUG: Parsed JSON array to Float[]: {}", java.util.Arrays.toString(floatArray));
//
//                                // Create SQL Array from Float[]
//                                java.sql.Array sqlArray = connection.createArrayOf("REAL", floatArray);
//                                stmt.setArray(parameterIndex, sqlArray);
//                                log.info("DEBUG: Successfully set REAL array parameter {} with setArray using Float[]: {}",
//                                        parameterIndex, java.util.Arrays.toString(floatArray));
//                            } else {
//                                // Handle NUMERIC arrays using setString with JSON format (Firebolt JDBC doesn't support NUMERIC array type)
//                                stmt.setString(parameterIndex, arrayJsonString);
//                                log.info("DEBUG: Successfully set NUMERIC array parameter {} with setString using JSON: '{}'",
//                                        parameterIndex, arrayJsonString);
//                            }
//                        } else {
//                            // Use setString for non-numeric arrays
//                            stmt.setString(parameterIndex, arrayJsonString);
//                            log.info("DEBUG: Successfully set non-numeric array parameter {} with setString: '{}'",
//                                    parameterIndex, arrayJsonString);
//                        }
//                    } else {
//                        // If for some reason we get a non-string array value, convert to JSON string
//                        try {
//                            String arrayJsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value);
//                            log.info("DEBUG: Converting non-string array value '{}' (type: {}) to JSON string for column '{}': '{}'",
//                                    value, value.getClass().getSimpleName(), columnName, arrayJsonString);
//                            stmt.setString(parameterIndex, arrayJsonString);
//                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
//                            log.error("Failed to serialize array to JSON for column '{}': {}", columnName, e.getMessage());
//                            // Fall back to toString() if JSON serialization fails
//                            String arrayString = value.toString();
//                            log.info("DEBUG: Falling back to toString() for array value '{}' (type: {}) for column '{}'",
//                                    value, value.getClass().getSimpleName(), columnName);
//                            stmt.setString(parameterIndex, arrayString);
//                        }
//                    }
//                } else if (isStructColumn(column)) {
//                    // Handle STRUCT columns by always ensuring we have a JSON string
//                    String jsonString = null;
//
//                    if (value instanceof Map) {
//                        try {
//                            // Convert Map to JSON string for Firebolt STRUCT
//                            jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value);
//                            log.info("DEBUG: Converted Map to JSON string for STRUCT column '{}': '{}'",
//                                    columnName, jsonString);
//                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
//                            log.error("Failed to serialize Map to JSON for STRUCT column '{}': {}", columnName, e.getMessage());
//                            throw new SQLException("Failed to serialize STRUCT Map to JSON", e);
//                        }
//                    } else if (value instanceof String) {
//                        // Already a JSON string, use as-is
//                        jsonString = (String) value;
//                        log.info("DEBUG: Using existing JSON string for STRUCT column '{}': '{}'",
//                                columnName, jsonString);
//                    } else {
//                        // For any other object type (including TestStruct), serialize to JSON
//                        try {
//                            jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value);
//                            log.info("DEBUG: Serialized {} object to JSON string for STRUCT column '{}': '{}'",
//                                    value.getClass().getSimpleName(), columnName, jsonString);
//                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
//                            log.error("Failed to serialize {} object to JSON for STRUCT column '{}': {}",
//                                    value.getClass().getSimpleName(), columnName, e.getMessage());
//                            throw new SQLException("Failed to serialize STRUCT object to JSON", e);
//                        }
//                    }
//
//                    // Always use setString with the JSON string
//                    stmt.setString(parameterIndex, jsonString);
//                    log.info("DEBUG: Successfully set STRUCT parameter {} with JSON string: '{}'",
//                            parameterIndex, jsonString);
//                } else {
//                    // Handle timezone-aware timestamps specially
//                    if (value instanceof String && isTimestampColumn(column)) {
//                        String stringValue = (String) value;
//                        if (stringValue.contains("T") && (stringValue.contains("+") || stringValue.contains("Z"))) {
//                            log.info("DEBUG: Converting timestamp string '{}' for proper timezone handling", stringValue);
//                            try {
//                                // Parse as OffsetDateTime first to handle timezone properly
//                                java.time.OffsetDateTime offsetDateTime = java.time.OffsetDateTime.parse(stringValue);
//                                log.info("DEBUG: Successfully parsed '{}' as OffsetDateTime: '{}'", stringValue, offsetDateTime);
//
//                                // Convert to UTC instant and then to Timestamp
//                                java.time.Instant utcInstant = offsetDateTime.toInstant();
//                                java.sql.Timestamp utcTimestamp = java.sql.Timestamp.from(utcInstant);
//                                log.info("DEBUG: Converted to UTC Timestamp: '{}'", utcTimestamp);
//
//                                // Use UTC Calendar to explicitly set timezone context
//                                java.util.Calendar utcCalendar = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
//                                stmt.setTimestamp(parameterIndex, utcTimestamp, utcCalendar);
//                                log.info("DEBUG: Set timestamp parameter {} with UTC Calendar", parameterIndex);
//                            } catch (java.time.format.DateTimeParseException e) {
//                                log.warn("Failed to parse timestamp string '{}' as OffsetDateTime: {}", stringValue, e.getMessage());
//                                // Fall back to original string value
//                                stmt.setObject(parameterIndex, value);
//                            }
//                        } else {
//                            stmt.setObject(parameterIndex, value);
//                        }
//                    } else if (isRealColumn(column)) {
//                        // Handle REAL columns specifically to avoid double precision conversion issues
//                        if (value instanceof Float) {
//                            Float floatValue = (Float) value;
//                            Float validatedValue = convertToValidRealValue(floatValue, columnName);
//                            stmt.setFloat(parameterIndex, validatedValue);
//                            log.info("DEBUG: ✅ Setting REAL parameter {} for column '{}' with setFloat using Float value: {} (validated to: {})",
//                                    parameterIndex, columnName, floatValue, validatedValue);
//                        } else if (value instanceof Double) {
//                            // Convert Double to Float for REAL columns
//                            Double doubleValue = (Double) value;
//                            Float floatValue = doubleValue.floatValue();
//                            Float validatedValue = convertToValidRealValue(floatValue, columnName);
//                            stmt.setFloat(parameterIndex, validatedValue);
//                            log.info("DEBUG: ✅ Setting REAL parameter {} for column '{}' with setFloat using converted Double->Float value: {} -> {} (validated to: {})",
//                                    parameterIndex, columnName, doubleValue, floatValue, validatedValue);
//                        } else {
//                            // For other types, convert to Float if possible
//                            try {
//                                Float floatValue = Float.parseFloat(value.toString());
//                                Float validatedValue = convertToValidRealValue(floatValue, columnName);
//                                stmt.setFloat(parameterIndex, validatedValue);
//                                log.info("DEBUG: ✅ Setting REAL parameter {} for column '{}' with setFloat using parsed value: {} -> {} (validated to: {})",
//                                        parameterIndex, columnName, value, floatValue, validatedValue);
//                            } catch (NumberFormatException e) {
//                                log.warn("Failed to convert value '{}' to Float for REAL column '{}': {}", value, columnName, e.getMessage());
//                                stmt.setObject(parameterIndex, value);
//                            }
//                        }
//                    } else if (isNumericColumn(column)) {
//                        // Handle NUMERIC columns specially to preserve precision
//                        if (value instanceof BigDecimal) {
//                            BigDecimal decimalValue = (BigDecimal) value;
//                            log.info("DEBUG: Setting NUMERIC parameter {} for column '{}' with BigDecimal value: '{}'",
//                                    parameterIndex, columnName, decimalValue);
//                            stmt.setString(parameterIndex, decimalValue.toString());
//                            log.info("DEBUG: Successfully set BigDecimal parameter {} with value: '{}'",
//                                    parameterIndex, decimalValue);
//                        } else if (value instanceof String) {
//                            String stringValue = (String) value;
//                            log.info("DEBUG: Setting NUMERIC parameter {} for column '{}' with string value: '{}'",
//                                    parameterIndex, columnName, stringValue);
//                            try {
//                                // For NUMERIC columns, use setString to preserve full precision
//                                // JDBC BigDecimal conversion can introduce precision loss
//                                BigDecimal decimalValue = new BigDecimal(stringValue);
//                                log.info("DEBUG: Validated string '{}' as valid BigDecimal (precision: {}, scale: {})",
//                                        stringValue, decimalValue.precision(), decimalValue.scale());
//                                stmt.setString(parameterIndex, stringValue);
//                                log.info("DEBUG: Successfully set NUMERIC parameter {} with string value: '{}'",
//                                        parameterIndex, stringValue);
//                            } catch (NumberFormatException e) {
//                                log.warn("Failed to parse numeric string '{}' as BigDecimal: {}", stringValue, e.getMessage());
//                                // Fall back to string value
//                                stmt.setString(parameterIndex, stringValue);
//                            }
//                        } else if (value instanceof Double || value instanceof Float) {
//                            // Log detailed information about the value type and column
//                            log.info("DEBUG: Processing numeric value for column '{}' (column type: '{}')",
//                                    columnName, column.getDataType());
//                            log.info("DEBUG: Value class: {}, Value: {}, Value as String: '{}'",
//                                    value.getClass().getSimpleName(), value, value.toString());
//
//                            // Check for extreme values that might cause issues
//                            if (value instanceof Float) {
//                                Float floatVal = (Float) value;
//                                if (floatVal.equals(Float.MIN_VALUE)) {
//                                    log.info("DEBUG: ⚠️  DETECTED Float.MIN_VALUE: {} in column '{}'", floatVal, columnName);
//                                } else if (floatVal.equals(Float.MAX_VALUE)) {
//                                    log.info("DEBUG: ⚠️  DETECTED Float.MAX_VALUE: {} in column '{}'", floatVal, columnName);
//                                }
//                            } else if (value instanceof Double) {
//                                Double doubleVal = (Double) value;
//                                if (doubleVal.equals((double) Float.MIN_VALUE)) {
//                                    log.info("DEBUG: ⚠️  DETECTED Double containing Float.MIN_VALUE: {} in column '{}'", doubleVal, columnName);
//                                } else if (doubleVal.equals((double) Float.MAX_VALUE)) {
//                                    log.info("DEBUG: ⚠️  DETECTED Double containing Float.MAX_VALUE: {} in column '{}'", doubleVal, columnName);
//                                }
//                            }
//
//                            // For REAL columns with Float values, use setFloat to avoid double precision conversion
//                            if (value instanceof Float && column.getDataType().toLowerCase().contains("real")) {
//                                Float floatValue = (Float) value;
//                                stmt.setFloat(parameterIndex, floatValue);
//                                log.info("DEBUG: ✅ Setting REAL parameter {} for column '{}' with setFloat using Float value: {}",
//                                        parameterIndex, columnName, floatValue);
//                            } else if (value instanceof Double && column.getDataType().toLowerCase().contains("real")) {
//                                // Convert Double to Float for REAL columns
//                                Double doubleValue = (Double) value;
//                                Float floatValue = doubleValue.floatValue();
//                                stmt.setFloat(parameterIndex, floatValue);
//                                log.info("DEBUG: ✅ Setting REAL parameter {} for column '{}' with setFloat using converted Double->Float value: {} -> {}",
//                                        parameterIndex, columnName, doubleValue, floatValue);
//                            } else {
//                                // Handle case where JSON deserialization converted BigDecimal to Double
//                                // For high-precision values, we should convert via string to avoid precision loss
//                                String stringValue = value.toString();
//                                log.info("DEBUG: Setting NUMERIC parameter {} for column '{}' with Double/Float value converted to string: '{}'",
//                                        parameterIndex, columnName, stringValue);
//                                try {
//                                    BigDecimal decimalValue = new BigDecimal(stringValue);
//                                    stmt.setBigDecimal(parameterIndex, decimalValue);
//                                    log.info("DEBUG: ✅ Successfully set BigDecimal parameter {} with value: '{}'",
//                                            parameterIndex, decimalValue);
//                                } catch (NumberFormatException e) {
//                                    log.warn("❌ Failed to parse numeric value '{}' as BigDecimal: {}", stringValue, e.getMessage());
//                                    // Fall back to original value
//                                    stmt.setObject(parameterIndex, value);
//                                }
//                            }
//                        } else {
//                            // For other numeric types, use setObject
//                            log.info("DEBUG: Setting NUMERIC parameter {} for column '{}' with other numeric type: '{}' (type: {})",
//                                    parameterIndex, columnName, value, value.getClass().getSimpleName());
//                            stmt.setObject(parameterIndex, value);
//                        }
//                    } else if (isByteaColumn(column)) {
//                        // Handle BYTEA columns
//                        if (value instanceof byte[]) {
//                            // Raw byte array - use directly
//                            byte[] byteValue = (byte[]) value;
//                            stmt.setBytes(parameterIndex, byteValue);
//                        } else if (value instanceof String) {
//                            // Base64 encoded string - decode first
//                            String base64String = (String) value;
//                            byte[] decoded = java.util.Base64.getDecoder().decode(base64String);
//                            stmt.setBytes(parameterIndex, decoded);
//                        } else {
//                            throw new SQLException("Unsupported BYTEA value type: " + value.getClass().getSimpleName());
//                        }
//                    } else if (isDateColumn(column)) {
//                        // Handle DATE columns - convert integers (days since epoch) to java.sql.Date
//                        log.info("DEBUG: Processing DATE column '{}' with value: '{}' (type: {})",
//                                columnName, value, value != null ? value.getClass().getSimpleName() : "null");
//
//                        if (value instanceof Integer) {
//                            // Kafka Connect Date logical type: Integer representing days since Unix epoch
//                            Integer daysSinceEpoch = (Integer) value;
//                            log.info("DEBUG: Converting Kafka Connect Date logical type for column '{}' - days since epoch: {}",
//                                    columnName, daysSinceEpoch);
//
//                            // Convert days since epoch to milliseconds since epoch
//                            long millisSinceEpoch = daysSinceEpoch.longValue() * 24L * 60L * 60L * 1000L;
//
//                            // Create java.sql.Date for Firebolt DATE column
//                            java.sql.Date sqlDate = new java.sql.Date(millisSinceEpoch);
//                            stmt.setDate(parameterIndex, sqlDate);
//                            log.info("DEBUG: ✅ Successfully set DATE parameter {} for column '{}' with java.sql.Date: {}",
//                                    parameterIndex, columnName, sqlDate);
//                        } else if (value instanceof java.sql.Date) {
//                            // Already the correct type
//                            java.sql.Date sqlDate = (java.sql.Date) value;
//                            stmt.setDate(parameterIndex, sqlDate);
//                            log.info("DEBUG: ✅ Set DATE parameter {} for column '{}' with existing java.sql.Date: {}",
//                                    parameterIndex, columnName, sqlDate);
//                        } else if (value instanceof java.util.Date) {
//                            // Convert java.util.Date to java.sql.Date
//                            java.util.Date utilDate = (java.util.Date) value;
//                            java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//                            stmt.setDate(parameterIndex, sqlDate);
//                            log.info("DEBUG: ✅ Set DATE parameter {} for column '{}' with converted java.sql.Date: {}",
//                                    parameterIndex, columnName, sqlDate);
//                        } else if (value instanceof String) {
//                            // Try to parse string as date
//                            try {
//                                java.sql.Date sqlDate = java.sql.Date.valueOf((String) value);
//                                stmt.setDate(parameterIndex, sqlDate);
//                                log.info("DEBUG: ✅ Set DATE parameter {} for column '{}' with parsed java.sql.Date: {}",
//                                        parameterIndex, columnName, sqlDate);
//                            } catch (IllegalArgumentException e) {
//                                log.error("Failed to parse date string '{}' for DATE column '{}': {}", value, columnName, e.getMessage());
//                                throw new SQLException("Invalid date format for DATE column: " + value, e);
//                            }
//                        } else {
//                            log.error("Unsupported value type '{}' for DATE column '{}': {}",
//                                    value.getClass().getSimpleName(), columnName, value);
//                            throw new SQLException("Unsupported value type for DATE column: " + value.getClass().getSimpleName());
//                        }
//                    } else if (isTimestampColumn(column)) {
//                        // Handle TIMESTAMP columns - convert longs (milliseconds since epoch) to java.sql.Timestamp
//                        log.info("DEBUG: Processing TIMESTAMP column '{}' with value: '{}' (type: {})",
//                                columnName, value, value != null ? value.getClass().getSimpleName() : "null");
//
//                        if (value instanceof Long) {
//                            Long epochValue = (Long) value;
//
//                            // Detect if this is microsecond precision based on column name
//                            if (columnName.toLowerCase().contains("microsecond")) {
//                                // Handle microsecond precision: Long representing microseconds since Unix epoch
//                                log.info("DEBUG: Converting microsecond precision timestamp for column '{}' - microseconds since epoch: {}",
//                                        columnName, epochValue);
//
//                                // Convert microseconds to milliseconds and nanoseconds for java.sql.Timestamp
//                                long millisSinceEpoch = epochValue / 1000; // Convert microseconds to milliseconds
//                                int remainingMicros = (int) (epochValue % 1000000); // Remaining microseconds within the second
//                                int totalNanos = remainingMicros * 1000; // Convert microseconds to nanoseconds
//
//                                java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(millisSinceEpoch);
//                                sqlTimestamp.setNanos(totalNanos); // Set sub-second precision
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Successfully set TIMESTAMP parameter {} for column '{}' with microsecond precision: {} ({}μs -> {}ms + {}ns)",
//                                        parameterIndex, columnName, sqlTimestamp, epochValue, millisSinceEpoch, totalNanos);
//                            } else {
//                                // Standard Kafka Connect Timestamp logical type: Long representing milliseconds since Unix epoch
//                                log.info("DEBUG: Converting Kafka Connect Timestamp logical type for column '{}' - millis since epoch: {}",
//                                        columnName, epochValue);
//
//                                // Create java.sql.Timestamp for Firebolt TIMESTAMP column
//                                java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(epochValue);
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Successfully set TIMESTAMP parameter {} for column '{}' with java.sql.Timestamp: {}",
//                                        parameterIndex, columnName, sqlTimestamp);
//                            }
//                        } else if (value instanceof java.sql.Timestamp) {
//                            // Already the correct type
//                            java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) value;
//                            stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                            log.info("DEBUG: ✅ Set TIMESTAMP parameter {} for column '{}' with existing java.sql.Timestamp: {}",
//                                    parameterIndex, columnName, sqlTimestamp);
//                        } else if (value instanceof java.util.Date) {
//                            // Convert java.util.Date to java.sql.Timestamp
//                            java.util.Date utilDate = (java.util.Date) value;
//                            java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
//                            stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                            log.info("DEBUG: ✅ Set TIMESTAMP parameter {} for column '{}' with converted java.sql.Timestamp: {}",
//                                    parameterIndex, columnName, sqlTimestamp);
//                        } else if (value instanceof String) {
//                            // Try to parse string as timestamp
//                            try {
//                                java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf((String) value);
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Set TIMESTAMP parameter {} for column '{}' with parsed java.sql.Timestamp: {}",
//                                        parameterIndex, columnName, sqlTimestamp);
//                            } catch (IllegalArgumentException e) {
//                                log.error("Failed to parse timestamp string '{}' for TIMESTAMP column '{}': {}", value, columnName, e.getMessage());
//                                throw new SQLException("Invalid timestamp format for TIMESTAMP column: " + value, e);
//                            }
//                        } else {
//                            log.error("Unsupported value type '{}' for TIMESTAMP column '{}': {}",
//                                    value.getClass().getSimpleName(), columnName, value);
//                            throw new SQLException("Unsupported value type for TIMESTAMP column: " + value.getClass().getSimpleName());
//                        }
//                    } else if (isTimestamptzColumn(column)) {
//                        // Handle TIMESTAMPTZ columns - convert longs (milliseconds since epoch) to java.sql.Timestamp
//                        log.info("DEBUG: Processing TIMESTAMPTZ column '{}' with value: '{}' (type: {})",
//                                columnName, value, value != null ? value.getClass().getSimpleName() : "null");
//
//                        if (value instanceof Long) {
//                            Long epochValue = (Long) value;
//
//                            // Detect if this is microsecond precision based on column name
//                            if (columnName.toLowerCase().contains("microsecond")) {
//                                // Handle microsecond precision: Long representing microseconds since Unix epoch
//                                log.info("DEBUG: Converting microsecond precision timestamptz for column '{}' - microseconds since epoch: {}",
//                                        columnName, epochValue);
//
//                                // Convert microseconds to milliseconds and nanoseconds for java.sql.Timestamp
//                                long millisSinceEpoch = epochValue / 1000; // Convert microseconds to milliseconds
//                                int remainingMicros = (int) (epochValue % 1000000); // Remaining microseconds within the second
//                                int totalNanos = remainingMicros * 1000; // Convert microseconds to nanoseconds
//
//                                java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(millisSinceEpoch);
//                                sqlTimestamp.setNanos(totalNanos); // Set precise nanoseconds for microsecond precision
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Successfully set TIMESTAMPTZ parameter {} for column '{}' with microsecond precision: {} ({}μs -> {}ms + {}ns)",
//                                        parameterIndex, columnName, sqlTimestamp, epochValue, millisSinceEpoch, totalNanos);
//                            } else {
//                                // Kafka Connect Timestamp logical type: Long representing milliseconds since Unix epoch
//                                log.info("DEBUG: Converting Kafka Connect Timestamp logical type for TIMESTAMPTZ column '{}' - millis since epoch: {}",
//                                        columnName, epochValue);
//
//                                // Create java.sql.Timestamp for Firebolt TIMESTAMPTZ column
//                                java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(epochValue);
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Successfully set TIMESTAMPTZ parameter {} for column '{}' with java.sql.Timestamp: {}",
//                                        parameterIndex, columnName, sqlTimestamp);
//                            }
//                        } else if (value instanceof java.sql.Timestamp) {
//                            // Already the correct type
//                            java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) value;
//                            stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                            log.info("DEBUG: ✅ Set TIMESTAMPTZ parameter {} for column '{}' with existing java.sql.Timestamp: {}",
//                                    parameterIndex, columnName, sqlTimestamp);
//                        } else if (value instanceof java.util.Date) {
//                            // Convert java.util.Date to java.sql.Timestamp
//                            java.util.Date utilDate = (java.util.Date) value;
//                            java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
//                            stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                            log.info("DEBUG: ✅ Set TIMESTAMPTZ parameter {} for column '{}' with converted java.sql.Timestamp: {}",
//                                    parameterIndex, columnName, sqlTimestamp);
//                        } else if (value instanceof String) {
//                            // Try to parse string as timestamp
//                            try {
//                                String timestampString = (String) value;
//                                java.sql.Timestamp sqlTimestamp;
//
//                                // Check if it's an ISO 8601 format with timezone (including Z for UTC)
//                                if (timestampString.contains("T") && (timestampString.contains("+") || timestampString.contains("Z") || (timestampString.contains("-") && timestampString.lastIndexOf("-") > 10))) {
//                                    // Parse ISO 8601 format with timezone - handle various formats including Z timezone
//                                    try {
//                                        // Use Instant.parse which handles most ISO 8601 formats including Z timezone
//                                        java.time.Instant instant = java.time.Instant.parse(timestampString);
//                                        sqlTimestamp = java.sql.Timestamp.from(instant);
//                                    } catch (java.time.format.DateTimeParseException e) {
//                                        // Fallback to ZonedDateTime parsing for edge cases
//                                        try {
//                                            // Try with microseconds (6 digits) and flexible timezone
//                                            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS[XXX][X]");
//                                            java.time.ZonedDateTime zonedDateTime = java.time.ZonedDateTime.parse(timestampString, formatter);
//                                            sqlTimestamp = java.sql.Timestamp.from(zonedDateTime.toInstant());
//                                        } catch (java.time.format.DateTimeParseException e2) {
//                                            try {
//                                                // Try with milliseconds (3 digits) and flexible timezone
//                                                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS[XXX][X]");
//                                                java.time.ZonedDateTime zonedDateTime = java.time.ZonedDateTime.parse(timestampString, formatter);
//                                                sqlTimestamp = java.sql.Timestamp.from(zonedDateTime.toInstant());
//                                            } catch (java.time.format.DateTimeParseException e3) {
//                                                // Final fallback - try without fractional seconds
//                                                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[XXX][X]");
//                                                java.time.ZonedDateTime zonedDateTime = java.time.ZonedDateTime.parse(timestampString, formatter);
//                                                sqlTimestamp = java.sql.Timestamp.from(zonedDateTime.toInstant());
//                                            }
//                                        }
//                                    }
//                                } else {
//                                    // Try standard SQL timestamp format
//                                    sqlTimestamp = java.sql.Timestamp.valueOf(timestampString);
//                                }
//
//                                stmt.setTimestamp(parameterIndex, sqlTimestamp);
//                                log.info("DEBUG: ✅ Set TIMESTAMPTZ parameter {} for column '{}' with parsed java.sql.Timestamp: {}",
//                                        parameterIndex, columnName, sqlTimestamp);
//                            } catch (Exception e) {
//                                log.error("Failed to parse timestamp string '{}' for TIMESTAMPTZ column '{}': {}", value, columnName, e.getMessage());
//                                throw new SQLException("Invalid timestamp format for TIMESTAMPTZ column: " + value, e);
//                            }
//                        } else {
//                            log.error("Unsupported value type '{}' for TIMESTAMPTZ column '{}': {}",
//                                    value.getClass().getSimpleName(), columnName, value);
//                            throw new SQLException("Unsupported value type for TIMESTAMPTZ column: " + value.getClass().getSimpleName());
//                        }
//                    } else {
//                        // General case - handle Map objects specially since Firebolt JDBC doesn't support them
//                        if (value instanceof Map) {
//                            try {
//                                // Serialize Map to JSON string as fallback
//                                String jsonString = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value);
//                                log.info("DEBUG: Fallback: Converting Map to JSON string for column '{}': '{}'", columnName, jsonString);
//                                stmt.setString(parameterIndex, jsonString);
//                            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
//                                log.error("Failed to serialize Map to JSON for column '{}': {}", columnName, e.getMessage());
//                                throw new SQLException("Failed to serialize Map to JSON", e);
//                            }
//                        } else {
//                            stmt.setObject(parameterIndex, value);
//                        }
//                    }
//                }
//
//                parameterIndex++;
//            }
//        }
//
//        // DEBUG: Log total parameters set
//        log.info("DEBUG: Set {} parameters for statement", parameterIndex - 1);
//    }

    /**
     * Checks if a column is a timestamp column based on its type.
     */
    private boolean isTimestampColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.equals("timestamp");
    }

    /**
     * Checks if a column is a TIMESTAMPTZ column based on its type.
     */
    private boolean isTimestamptzColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.equals("timestamptz");
    }

    /**
     * Checks if a column is a REAL column based on its type.
     */
    private boolean isRealColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.contains("real") || dataType.contains("float");
    }

    /**
     * Checks if a column is a DATE column based on its type.
     */
    private boolean isDateColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.equals("date");
    }

    /**
     * Validates if a Float value is within Firebolt's REAL column supported range.
     * Firebolt REAL columns have limitations on extreme values like Float.MIN_VALUE and Float.MAX_VALUE.
     */
    private boolean isValidRealValue(Float value) {
        if (value == null) {
            return true; // NULL is always valid
        }

        // Check for problematic extreme values that Firebolt REAL cannot handle
        if (value.equals(Float.MIN_VALUE) || value.equals(Float.MAX_VALUE)) {
            return false;
        }

        // Check for infinity and NaN
        if (Float.isInfinite(value) || Float.isNaN(value)) {
            return false;
        }

        return true;
    }

    /**
     * Converts an invalid Float value to a valid one for Firebolt REAL columns.
     * This handles extreme values by clamping them to a safe range.
     */
    private Float convertToValidRealValue(Float value, String columnName) {
        log.info("🔍 VALIDATION: Checking Float value for REAL column '{}': {} ({})",
                columnName, value, value != null ? value.getClass().getSimpleName() : "null");

        if (value == null) {
            log.info("✅ VALIDATION: Null value passed through for REAL column '{}'", columnName);
            return null;
        }

        if (Float.isNaN(value)) {
            log.warn("Converting NaN to NULL for REAL column '{}': {}", columnName, value);
            return null;
        }

        if (Float.isInfinite(value)) {
            log.warn("Converting infinite value to NULL for REAL column '{}': {}", columnName, value);
            return null;
        }

        if (value.equals(Float.MIN_VALUE)) {
            // Use smallest positive normal float instead of MIN_VALUE (which is smallest positive subnormal)
            Float safeValue = Float.MIN_NORMAL;
            log.warn("⚠️ VALIDATION: Converting Float.MIN_VALUE to safe value for REAL column '{}': {} -> {}",
                    columnName, value, safeValue);
            return safeValue;
        }

        if (value.equals(Float.MAX_VALUE)) {
            // Use extremely conservative value - orders of magnitude smaller to ensure Firebolt compatibility
            Float safeValue = 999999.9f; // Very conservative value well within normal float range
            log.warn("⚠️ VALIDATION: Converting Float.MAX_VALUE to safe value for REAL column '{}': {} -> {}",
                    columnName, value, safeValue);
            return safeValue;
        }

        log.info("✅ VALIDATION: Float value is valid for REAL column '{}': {}", columnName, value);
        return value; // Value is already valid
    }

    /**
     * Checks if a column is a numeric column based on its type.
     */
    private boolean isNumericColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.contains("numeric") || dataType.contains("decimal");
    }

    /**
     * Checks if a column is a BYTEA column based on its type.
     */
    private boolean isByteaColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.contains("bytea");
    }

    /**
     * Checks if a column is an array column based on its type.
     */
    private boolean isArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.startsWith("array(") || dataType.contains("[]");
    }

    /**
     * Checks if a column is specifically a DATE array column (ARRAY(DATE)).
     */
    private boolean isDateArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.startsWith("array(") && dataType.contains("date");
    }

    /**
     * Checks if a column is specifically a TIMESTAMP array column (ARRAY(TIMESTAMP)).
     */
    private boolean isTimestampArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.startsWith("array(") && dataType.contains("timestamp") && !dataType.contains("timestamptz");
    }

    /**
     * Checks if a column is specifically a TIMESTAMPTZ array column (ARRAY(TIMESTAMPTZ)).
     */
    private boolean isTimestamptzArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        return dataType.startsWith("array(") && dataType.contains("timestamptz");
    }

    /**
     * Checks if a column is specifically a numeric array column (ARRAY(NUMERIC), ARRAY(INTEGER), ARRAY(BIGINT), or ARRAY(REAL)).
     * Excludes nested arrays (ARRAY(ARRAY(...))).
     */
    private boolean isNumericArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        log.error("DEBUG: isNumericArrayColumn called for column '{}' with dataType '{}'", column.getName(), column.getDataType());
        log.info("DEBUG: isNumericArrayColumn check for column '{}' with dataType '{}'", column.getName(), column.getDataType());

        // First check if it's a nested array - if so, it's not a simple numeric array
        int arrayCount = 0;
        int index = 0;
        while ((index = dataType.indexOf("array(", index)) != -1) {
            arrayCount++;
            index += 6; // length of "array("
        }
        if (arrayCount >= 2) {
            log.error("DEBUG: Column '{}' is a nested array (array count: {}), returning false", column.getName(), arrayCount);
            log.info("DEBUG: Column '{}' is a nested array (array count: {}), returning false", column.getName(), arrayCount);
            return false;
        }

        boolean isNumeric = dataType.startsWith("array(") && (dataType.contains("numeric") || dataType.contains("bigint") || dataType.contains("real") || dataType.contains("float"));
        log.error("DEBUG: Column '{}' isNumeric result: {}", column.getName(), isNumeric);
        log.info("DEBUG: Column '{}' isNumeric result: {}", column.getName(), isNumeric);
        return isNumeric;
    }

    /**
     * Checks if a column is specifically an integer array column (ARRAY(INTEGER)).
     * Excludes nested arrays and other numeric types.
     */
    private boolean isIntegerArrayColumn(TableSchema.Column column) {
        log.error("DEBUG: isIntegerArrayColumn called for column '{}' with dataType '{}'", column.getName(), column.getDataType());
        String dataType = column.getDataType().toLowerCase();

        // First check if it's a nested array - if so, it's not a simple integer array
        int arrayCount = 0;
        int index = 0;
        while ((index = dataType.indexOf("array(", index)) != -1) {
            arrayCount++;
            index += 6; // length of "array("
        }
        if (arrayCount >= 2) {
            log.error("DEBUG: Column '{}' is a nested array (array count: {}), returning false for integer array check", column.getName(), arrayCount);
            return false;
        }

        boolean isInteger = dataType.startsWith("array(") && (dataType.contains("integer") || dataType.contains("int")) && !dataType.contains("bigint");
        log.error("DEBUG: Column '{}' isInteger result: {}", column.getName(), isInteger);
        return isInteger;
    }

    /**
     * Checks if a column is a nested array (array of arrays).
     * Example: ARRAY(ARRAY(INTEGER)), ARRAY(ARRAY(TEXT)), etc.
     */
    private boolean isNestedArrayColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        // Look for pattern like "array(array(" or "array(array("
        // More robust check: count the number of "array(" occurrences
        int arrayCount = 0;
        int index = 0;
        while ((index = dataType.indexOf("array(", index)) != -1) {
            arrayCount++;
            index += 6; // length of "array("
        }
        boolean isNested = arrayCount >= 2;
        log.info("DEBUG: isNestedArrayColumn check for column '{}' with dataType '{}': {} (array count: {})",
                column.getName(), column.getDataType(), isNested, arrayCount);
        log.error("DEBUG: isNestedArrayColumn check for column '{}' with dataType '{}': {} (array count: {})",
                column.getName(), column.getDataType(), isNested, arrayCount);
        return isNested;
    }

    /**
     * Determines if a column should be treated as a STRUCT (JSON) column.
     * Since Firebolt JDBC driver doesn't support STRUCT type, we use TEXT columns
     * but identify them by name pattern.
     */
    private boolean isStructColumn(TableSchema.Column column) {
        String dataType = column.getDataType().toLowerCase();
        String columnName = column.getName().toLowerCase();

        // Original STRUCT type detection (for future compatibility)
        if (dataType.startsWith("struct(")) {
            return true;
        }

        // TEXT columns that store JSON/STRUCT data (identified by name pattern)
        return columnName.contains("struct");
    }

    /**
     * Parses a JSON array string into an Integer array.
     * Example: '[1,null,3]' -> [1,null,3]
     */
    private Integer[] parseJsonArrayToIntegerArray(String jsonArrayString) {
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new Integer[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new Integer[0];
            }

            java.util.List<Integer> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                } else {
                    try {
                        Integer intValue = Integer.parseInt(element);
                        elements.add(intValue);
                    } catch (NumberFormatException e) {
                        log.warn("Failed to parse '{}' as integer in array, treating as null", element);
                        elements.add(null);
                    }
                }
            }

            return elements.toArray(new Integer[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Integer array: {}", jsonArrayString, e.getMessage());
            return new Integer[0];
        }
    }

    /**
     * Parses a JSON array string into a Long array.
     * Example: '[1,null,3]' -> [1,null,3]
     */
    private Long[] parseJsonArrayToLongArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToLongArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new Long[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new Long[0];
            }

            java.util.List<Long> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                } else {
                    try {
                        Long longValue = Long.parseLong(element);
                        elements.add(longValue);
                        log.info("DEBUG: Successfully parsed '{}' as Long: {}", element, longValue);
                    } catch (NumberFormatException e) {
                        log.warn("DEBUG: Failed to parse '{}' as long in array, treating as null. Error: {}", element, e.getMessage());
                        elements.add(null);
                    }
                }
            }

            return elements.toArray(new Long[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Long array: {}", jsonArrayString, e.getMessage());
            return new Long[0];
        }
    }

    /**
     * Parses a JSON array string into a Float array.
     * Example: '[1.5,null,3.14]' -> [1.5,null,3.14]
     */
    private Float[] parseJsonArrayToFloatArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToFloatArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new Float[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new Float[0];
            }

            java.util.List<Float> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                } else {
                    try {
                        Float floatValue = Float.parseFloat(element);
                        Float validatedValue = convertToValidRealValue(floatValue, "REAL array element");
                        elements.add(validatedValue);
                        log.info("DEBUG: Successfully parsed '{}' as Float: {} (validated to: {})", element, floatValue, validatedValue);
                    } catch (NumberFormatException e) {
                        log.warn("DEBUG: Failed to parse '{}' as float in array, treating as null. Error: {}", element, e.getMessage());
                        elements.add(null);
                    }
                }
            }

            return elements.toArray(new Float[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Float array: {}", jsonArrayString, e.getMessage());
            return new Float[0];
        }
    }

    /**
     * Parses a JSON array string into a String array.
     * Example: '["3.141592654","2.718281828"]' -> ["3.141592654","2.718281828"]
     */
    private String[] parseJsonArrayToStringArray(String jsonArrayString) {
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new String[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new String[0];
            }

            java.util.List<String> elements = new java.util.ArrayList<>();
            boolean inQuotes = false;
            StringBuilder current = new StringBuilder();

            for (int i = 0; i < content.length(); i++) {
                char c = content.charAt(i);

                if (c == '"' && (i == 0 || content.charAt(i-1) != '\\')) {
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    String element = current.toString().trim();
                    if (element.startsWith("\"") && element.endsWith("\"")) {
                        element = element.substring(1, element.length() - 1);
                    }
                    // Handle null values properly
                    if (element.equals("null") || element.equalsIgnoreCase("null")) {
                        elements.add(null);
                    } else {
                        elements.add(element);
                    }
                    current = new StringBuilder();
                } else if (c != '"' || inQuotes) {
                    current.append(c);
                }
            }

            // Add the last element
            if (current.length() > 0) {
                String element = current.toString().trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }
                // Handle null values properly
                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                } else {
                    elements.add(element);
                }
            }

            return elements.toArray(new String[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}': {}", jsonArrayString, e.getMessage());
            return new String[0];
        }
    }

    /**
     * Parses a JSON array string into a java.sql.Date array.
     * Converts integer elements (days since epoch) to java.sql.Date objects.
     * Example: '[19723,null,19724]' -> [2024-01-15,null,2024-01-16]
     */
    private java.sql.Date[] parseJsonArrayToDateArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToDateArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new java.sql.Date[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new java.sql.Date[0];
            }

            java.util.List<java.sql.Date> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                    log.info("DEBUG: Added null element to date array");
                } else {
                    try {
                        // Parse as integer (days since epoch) and convert to java.sql.Date
                        Integer daysSinceEpoch = Integer.parseInt(element);
                        long millisSinceEpoch = daysSinceEpoch.longValue() * 24L * 60L * 60L * 1000L;
                        java.sql.Date sqlDate = new java.sql.Date(millisSinceEpoch);
                        elements.add(sqlDate);
                        log.info("DEBUG: Successfully parsed '{}' as {} days since epoch -> Date: {}",
                                element, daysSinceEpoch, sqlDate);
                    } catch (NumberFormatException e) {
                        log.warn("DEBUG: Failed to parse '{}' as integer for date conversion, treating as null. Error: {}",
                                element, e.getMessage());
                        elements.add(null);
                    }
                }
            }

            return elements.toArray(new java.sql.Date[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Date array: {}", jsonArrayString, e.getMessage());
            return new java.sql.Date[0];
        }
    }

    /**
     * Parses a JSON array string into a java.sql.Timestamp array.
     * Converts long elements (milliseconds since epoch) to java.sql.Timestamp objects.
     * Example: '[1705334400000,null,1705420800000]' -> [2024-01-15 14:30:45,null,2024-01-16 16:45:30]
     */
    private java.sql.Timestamp[] parseJsonArrayToTimestampArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToTimestampArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new java.sql.Timestamp[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new java.sql.Timestamp[0];
            }

            java.util.List<java.sql.Timestamp> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                    log.info("DEBUG: Added null element to timestamp array");
                } else {
                    try {
                        // Try to parse as long first (milliseconds since epoch)
                        Long millisSinceEpoch = Long.parseLong(element);
                        java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(millisSinceEpoch);
                        elements.add(sqlTimestamp);
                        log.info("DEBUG: Successfully parsed '{}' as {} millis since epoch -> Timestamp: {}",
                                element, millisSinceEpoch, sqlTimestamp);
                    } catch (NumberFormatException e1) {
                        try {
                            // If parsing as long fails, try parsing as ISO-8601 timestamp string with microsecond precision
                            // Expected format: "2024-01-15T14:30:45.123456" or "2024-01-15 14:30:45.123456"
                            String normalizedElement = element.replace("T", " "); // Convert ISO format to SQL format
                            java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(normalizedElement);
                            elements.add(sqlTimestamp);
                            log.info("DEBUG: Successfully parsed '{}' as timestamp string -> Timestamp: {}",
                                    element, sqlTimestamp);
                        } catch (Exception e2) {
                            log.warn("DEBUG: Failed to parse '{}' as both long and timestamp string, treating as null. Long parse error: {}, String parse error: {}",
                                    element, e1.getMessage(), e2.getMessage());
                            elements.add(null);
                        }
                    }
                }
            }

            return elements.toArray(new java.sql.Timestamp[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Timestamp array: {}", jsonArrayString, e.getMessage());
            return new java.sql.Timestamp[0];
        }
    }

    /**
     * Parses a JSON array of arrays string into a nested Integer array (Integer[][]).
     * Handles null elements in both outer and inner arrays.
     */
    private Integer[][] parseJsonArrayToNestedIntegerArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToNestedIntegerArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new Integer[0][0];
        }

        try {
            // Parse the JSON array of arrays
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode rootNode = mapper.readTree(jsonArrayString);

            if (!rootNode.isArray()) {
                throw new IllegalArgumentException("Input is not a JSON array: " + jsonArrayString);
            }

            int outerSize = rootNode.size();
            Integer[][] result = new Integer[outerSize][];

            for (int i = 0; i < outerSize; i++) {
                com.fasterxml.jackson.databind.JsonNode innerNode = rootNode.get(i);

                if (innerNode.isNull()) {
                    result[i] = null;
                } else if (innerNode.isArray()) {
                    int innerSize = innerNode.size();
                    Integer[] innerArray = new Integer[innerSize];

                    for (int j = 0; j < innerSize; j++) {
                        com.fasterxml.jackson.databind.JsonNode elementNode = innerNode.get(j);

                        if (elementNode.isNull()) {
                            innerArray[j] = null;
                        } else if (elementNode.isInt()) {
                            innerArray[j] = elementNode.asInt();
                        } else if (elementNode.isLong()) {
                            long longValue = elementNode.asLong();
                            if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                                throw new NumberFormatException("Value " + longValue + " is outside Integer range");
                            }
                            innerArray[j] = (int) longValue;
                        } else {
                            throw new IllegalArgumentException("Invalid element type at position [" + i + "][" + j + "]: " + elementNode.getNodeType());
                        }
                    }

                    result[i] = innerArray;
                } else {
                    throw new IllegalArgumentException("Invalid inner element type at position " + i + ": " + innerNode.getNodeType());
                }
            }

            log.info("DEBUG: Successfully parsed nested array with {} outer elements", outerSize);
            return result;

        } catch (Exception e) {
            log.error("Failed to parse JSON array of arrays string '{}': {}", jsonArrayString, e.getMessage());
            throw new RuntimeException("Failed to parse JSON array of arrays", e);
        }
    }



    /**
     * Parses a JSON array string into a java.sql.Timestamp array for TIMESTAMPTZ columns.
     * Converts long elements (milliseconds since epoch) to java.sql.Timestamp objects.
     * Example: '[1705334400000,null,1705420800000]' -> [2024-01-15 14:30:45,null,2024-01-16 16:45:30]
     */
    private java.sql.Timestamp[] parseJsonArrayToTimestamptzArray(String jsonArrayString) {
        log.info("DEBUG: parseJsonArrayToTimestamptzArray called with input: '{}'", jsonArrayString);
        if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
            return new java.sql.Timestamp[0];
        }

        try {
            // Remove outer brackets and split by comma, handling quoted strings and nulls
            String content = jsonArrayString.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1);
            }

            if (content.trim().isEmpty()) {
                return new java.sql.Timestamp[0];
            }

            java.util.List<java.sql.Timestamp> elements = new java.util.ArrayList<>();
            String[] parts = content.split(",");

            for (String part : parts) {
                String element = part.trim();
                if (element.startsWith("\"") && element.endsWith("\"")) {
                    element = element.substring(1, element.length() - 1);
                }

                if (element.equals("null") || element.equalsIgnoreCase("null")) {
                    elements.add(null);
                    log.info("DEBUG: Added null element to timestamptz array");
                } else {
                    try {
                        // First try to parse as long (milliseconds or microseconds since epoch)
                        Long epochValue = Long.parseLong(element);
                        java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(epochValue);
                        elements.add(sqlTimestamp);
                        log.info("DEBUG: Successfully parsed '{}' as {} millis since epoch -> Timestamp: {}",
                                element, epochValue, sqlTimestamp);
                    } catch (NumberFormatException e1) {
                        try {
                            // If parsing as long fails, try parsing as ISO-8601 timestamp string with microsecond precision
                            // Convert ISO-8601 format to SQL timestamp format
                            String timestampStr = element.replace("T", " ").replace("+00:00", "");
                            java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(timestampStr);
                            elements.add(sqlTimestamp);
                            log.info("DEBUG: Successfully parsed '{}' as timestamptz string -> Timestamp: {}",
                                    element, sqlTimestamp);
                        } catch (Exception e2) {
                            log.warn("DEBUG: Failed to parse '{}' as both long and timestamptz string, treating as null. Long parse error: {}, String parse error: {}",
                                    element, e1.getMessage(), e2.getMessage());
                            elements.add(null);
                        }
                    }
                }
            }

            return elements.toArray(new java.sql.Timestamp[0]);

        } catch (Exception e) {
            log.warn("Failed to parse JSON array string '{}' as Timestamptz array: {}", jsonArrayString, e.getMessage());
            return new java.sql.Timestamp[0];
        }
    }

    /**
     * Computes the set of column names present in the given records.
     *
     * @param records the list of records to analyze
     * @return a set of column names found across all records
     */
    private Set<String> computeColumnNamesFromRecords(List<FireboltRecord> records) {
        Set<String> columnNames = new HashSet<>();

        for (FireboltRecord record : records) {
            columnNames.addAll(record.getColumnValues().keySet());
        }

        return columnNames;
    }

    /**
     * Filters the column names to only include those that exist in the table schema.
     * This ensures we don't try to insert into columns that don't exist in the target table.
     * Note: Uses case-insensitive matching since some databases normalize column names.
     *
     * @param recordColumnNames the column names from the records
     * @param tableSchema the table schema containing valid columns
     * @return a set of column names that exist in both the records and the table schema
     */
    private Set<String> filterValidColumns(Set<String> recordColumnNames, TableSchema tableSchema) {
        Set<String> validColumns = new HashSet<>();
        Map<String, String> schemaColumnNamesLowerCase = new HashMap<>();

        // Create a case-insensitive lookup map for schema column names
        for (TableSchema.Column column : tableSchema.getColumns()) {
            schemaColumnNamesLowerCase.put(column.getName().toLowerCase(), column.getName());
        }

        // Debug log all available schema columns
        log.info("DEBUG: Table '{}' schema has {} columns: {}", tableSchema.getTableName(),
                tableSchema.getColumns().size(), tableSchema.getColumns().stream()
                        .map(TableSchema.Column::getName).collect(java.util.stream.Collectors.toList()));

        // Debug log all record columns
        log.info("DEBUG: Record has {} columns: {}", recordColumnNames.size(), recordColumnNames);

        // Only keep column names that exist in both the records and the schema (case-insensitive)
        for (String columnName : recordColumnNames) {
            String lowerCaseColumnName = columnName.toLowerCase();
            if (schemaColumnNamesLowerCase.containsKey(lowerCaseColumnName)) {
                validColumns.add(columnName); // Keep the original case from the record
                log.debug("Column '{}' matched schema column '{}' (case-insensitive)",
                        columnName, schemaColumnNamesLowerCase.get(lowerCaseColumnName));
            } else {
                log.info("DEBUG: FILTERING OUT column '{}' as it doesn't exist in table schema for table '{}'",
                        columnName, tableSchema.getTableName());
            }
        }

        log.debug("Filtered {} record columns to {} valid columns for table '{}'",
                recordColumnNames.size(), validColumns.size(), tableSchema.getTableName());

        return validColumns;
    }
}