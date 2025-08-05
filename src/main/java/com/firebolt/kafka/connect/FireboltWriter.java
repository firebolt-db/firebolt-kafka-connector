package com.firebolt.kafka.connect;

import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverterFactory;
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
    private long lastFlushTime;

    public FireboltWriter(SinkConfig config, Connection connection) {
        this.config = config;
        this.connection = connection;
        this.recordBatch = new ArrayList<>();
        this.insertStatements = new ConcurrentHashMap<>();
        this.tableSchemas = new ConcurrentHashMap<>();
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

        log.info("DEBUG: Set {} parameters for statement", parameterIndex - 1);
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