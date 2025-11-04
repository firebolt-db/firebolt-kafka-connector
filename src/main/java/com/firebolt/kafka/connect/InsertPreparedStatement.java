package com.firebolt.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeFactoryProvider;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Insert into a table using prepared statements
 */
@Slf4j
public class InsertPreparedStatement {

    protected Connection connection;
    private TableSchema tableSchema;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;
    private long maxQuerySize;

    public InsertPreparedStatement(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll, long maxQuerySize) {
        this.connection = connection;
        this.tableSchema = tableSchema;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
        this.maxQuerySize = maxQuerySize;
    }

    public void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        // since no schema evolution is supported, keep track of all the column names that are present in the records and filter out the ones that are not part of the table schema column names
        Set<String> columnNamesFromRecords = computeColumnNamesFromRecords(fireboltRecords);

        // map of firebolt column names to record attribute name
        Map<String, String> validColumnNames = filterValidColumns(columnNamesFromRecords, tableSchema);

        // create the prepared statement and add the records
        addRecordsInternal(fireboltRecords, validColumnNames);
    }

    private void addRecordsInternal(List<AbstractFireboltRecord> fireboltRecords, Map<String, String> validColumnNames) throws SQLException {
        // If maxQuerySize is set, split records into batches based on query size
        if (maxQuerySize > 0) {
            List<List<AbstractFireboltRecord>> batches = splitIntoBatchesByQuerySize(fireboltRecords, validColumnNames);
            for (List<AbstractFireboltRecord> batch : batches) {
                executeBatch(batch, validColumnNames);
            }
        } else {
            executeBatch(fireboltRecords, validColumnNames);
        }
    }

    private void executeBatch(List<AbstractFireboltRecord> fireboltRecords, Map<String, String> validColumnNames) throws SQLException {
        try (PreparedStatement preparedStatement = createPreparedStatement(tableSchema, validColumnNames.keySet())) {

            // Execute batch insert
            for (AbstractFireboltRecord record : fireboltRecords) {
                try {
                    setStatementParameters(preparedStatement, record, tableSchema, validColumnNames);
                    preparedStatement.addBatch();
                } catch (RecordConversionFailedException e) {
                    if (errorToleranceAll) {
                        errorReporter.report(record.getSinkRecord(), e);
                        log.warn("Record from partition {} at offset {} will be submitted to the deadletter queue ", e.getKafkaPartition(), e.getKafkaOffset());
                    } else {
                        throw e;
                    }
                }
            }

            try {
                int[] results = preparedStatement.executeBatch();
                log.info("Batch insert results: {} rows affected", results.length);
            } catch (SQLException e) {
                if (!isHttpEntityTooLargeException(e)) {
                    throw e; // rethrow it as we only handle the entity too large here
                }

                // split the records in half and try to process the records
                if (fireboltRecords.size() == 1) {
                    AbstractFireboltRecord fireboltRecord = fireboltRecords.get(0);
                    log.warn("Cannot process the firebolt record from partition {} at offset {}, as it is too large and exceeds the Firebolt request entity size", fireboltRecord.getPartition(), fireboltRecord.getOffset());
                    if (errorToleranceAll) {
                        errorReporter.report(fireboltRecord.getSinkRecord(), e);
                    } else {
                        throw e;
                    }
                } else {
                    // simple strategy for now. Split the rows in two and try each half again
                    int leftSide = fireboltRecords.size() / 2;
                    log.warn("Batch size was too big so will split the records in smaller batches {} & {}", leftSide, fireboltRecords.size()-leftSide);
                    executeBatch(fireboltRecords.subList(0,leftSide), validColumnNames);
                    executeBatch(fireboltRecords.subList(leftSide,fireboltRecords.size()), validColumnNames);
                }
            } finally {
                // Clear batch
                preparedStatement.clearBatch();
                // once clear batch on firebolt prepared statements are implemented properly then we can remove this and only keep clearBatch()
                preparedStatement.clearParameters();
            }

        }
    }

    /**
     * For the HTTP Entity too large exception we have an exception type of 413.
     */
    private boolean isHttpEntityTooLargeException(SQLException e) {
        return (e instanceof FireboltException) && ((FireboltException) e).getType() == ExceptionType.REQUEST_BODY_TOO_LARGE;
    }

    private PreparedStatement createPreparedStatement(TableSchema tableSchema, Set<String> validColumnNames) throws SQLException {
        String insertSQL = buildInsertSQL(tableSchema, validColumnNames);
        log.debug("Prepared insert SQL: {}", insertSQL);

        return connection.prepareStatement(insertSQL);
    }

    private String buildInsertSQL(TableSchema tableSchema, Set<String> validColumnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO \"").append(tableSchema.getTableName()).append("\" (");

        List<String> columnNames = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();

        for (TableSchema.Column column : tableSchema.getColumns()) {
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

    private void setStatementParameters(PreparedStatement stmt, AbstractFireboltRecord record, TableSchema schema, Map<String, String> validColumnNames) throws SQLException, RecordConversionFailedException {
        int parameterIndex = 1;
        for (TableSchema.Column column : schema.getColumns()) {
            if (validColumnNames.containsKey(column.getName())) {
                String attributeName = validColumnNames.get(column.getName());
                KafkaMessageColumnValue kafkaMessageColumnValue = record.getColumnValue(attributeName);

                if (kafkaMessageColumnValue == null || kafkaMessageColumnValue.getValue() == null) {
                    stmt.setNull(parameterIndex, column.getSqlType());
                } else {
                    ColumnDataTypeConverter columnDataTypeConverter = ColumnDataTypeFactoryProvider.getInstance(record.hasValueSchema()).getConverter(column);
                    try {
                        columnDataTypeConverter.convertAndSet(stmt, parameterIndex, kafkaMessageColumnValue, column);
                    } catch (ColumnConversionFailedException e) {
                        log.error("Conversion failed for table {} column {} for kafka record from partition {} offset {}", schema.getTableName(), column.getName(), record.getPartition(), record.getOffset());

                        // as of now we are failing at the first column conversion failure. We could try to convert all the columns so we give all the data in one record convertion exception.
                        throw RecordConversionFailedException.builder()
                                .message(e.getMessage())
                                .tableName(schema.getTableName())
                                .kafkaPartition(record.getPartition())
                                .kafkaOffset(record.getOffset())
                                .topicName(record.getTopic())
                                .build();
                    }
                }

                parameterIndex++;
            }
        }
    }

    /**
     * Computes the set of column names present in the given records.
     *
     * @param records the list of records to analyze
     * @return a set of column names found across all records
     */
    protected Set<String> computeColumnNamesFromRecords(List<AbstractFireboltRecord> records) {
        Set<String> columnNames = new HashSet<>();

        for (AbstractFireboltRecord record : records) {
            columnNames.addAll(record.getColumnNames());
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
     * @return a map of column names that map from firebolt column name to record attribute name
     */
    private Map<String, String> filterValidColumns(Set<String> recordColumnNames, TableSchema tableSchema) {
        Map<String, String> validColumns = new HashMap<>();

        // map all table names to lower case
        Map<String, String> schemaColumnNamesLowerCase = tableSchema.getColumns()
                .stream()
                .map(TableSchema.Column::getName)
                .collect(Collectors.toMap(String::toLowerCase, Function.identity()));

        log.debug("Table '{}' schema has {} columns: {}", tableSchema.getTableName(),
                tableSchema.getColumns().size(), tableSchema.getColumns().stream()
                        .map(TableSchema.Column::getName).collect(java.util.stream.Collectors.toList()));

        log.debug("Record has {} columns: {}", recordColumnNames.size(), recordColumnNames);

        // Only keep column names that exist in both the record and the schema (case-insensitive)
        for (String columnName : recordColumnNames) {
            String lowerCaseColumnName = columnName.toLowerCase();
            if (schemaColumnNamesLowerCase.containsKey(lowerCaseColumnName)) {
                validColumns.put(schemaColumnNamesLowerCase.get(lowerCaseColumnName), columnName); // map the firebolt column name to record attribute name
                log.debug("Column '{}' matched schema column '{}' (case-insensitive)",
                        columnName, schemaColumnNamesLowerCase.get(lowerCaseColumnName));
            } else {
                log.warn("Column '{}' as it doesn't exist in table schema for table '{}'", columnName, tableSchema.getTableName());
            }
        }

        log.debug("Filtered {} record columns to {} valid columns for table '{}'. These are the valid column names {}",
                recordColumnNames.size(), validColumns.size(), tableSchema.getTableName(), validColumns.entrySet());

        return validColumns;
    }

    /**
     * Splits records into batches based on the maximum query size limit.
     * Each batch will be a separate INSERT statement concatenated with semicolons.
     * 
     * @param fireboltRecords the records to split
     * @param validColumnNames the valid column names for calculating query size
     * @return list of batches, where each batch respects the maxQuerySize limit
     */
    private List<List<AbstractFireboltRecord>> splitIntoBatchesByQuerySize(List<AbstractFireboltRecord> fireboltRecords, Map<String, String> validColumnNames) {
        String insertSQLTemplate = buildInsertSQL(tableSchema, validColumnNames.keySet());
        long templateSizeBytes = insertSQLTemplate.length();

        List<List<AbstractFireboltRecord>> batches = new ArrayList<>();
        List<AbstractFireboltRecord> currentBatch = new ArrayList<>();
        long currentBatchSize = 0;
        // we leave a certain overhead so that queries will not exceed max size
        long actualMaxQuerySizeBytes = (long) (maxQuerySize * 0.95);
        List<String> attributeNames = new ArrayList<>(validColumnNames.values());

        for (AbstractFireboltRecord record : fireboltRecords) {
            long recordParameterSize = calculateParameterSize(record, attributeNames);
            // Each INSERT statement: template + parameters
            long recordQuerySize = templateSizeBytes + recordParameterSize;

            // If adding this record would exceed the limit and we have records in the current batch, start a new batch
            if (!currentBatch.isEmpty() && currentBatchSize + recordQuerySize > actualMaxQuerySizeBytes) {
                batches.add(currentBatch);
                currentBatch = new ArrayList<>();
                currentBatchSize = 0;
            }

            currentBatch.add(record);
            currentBatchSize += recordQuerySize;
        }

        // Add the last batch if it has records
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        if (batches.size() > 1) {
            log.info("Split {} records into {} batches based on maxQuerySize limit of {} bytes", 
                    fireboltRecords.size(), batches.size(), maxQuerySize);
        }

        return batches;
    }

    /**
     * Calculates the estimated byte size of parameter values for a record.
     * This estimates how the parameters would be serialized in the final SQL query.
     * 
     * @param record the record to calculate parameter size for
     * @param attributeNames the list of attribute names to calculate size for
     * @return estimated byte size of the parameter values
     */
    private long calculateParameterSize(AbstractFireboltRecord record, List<String> attributeNames) {
        long totalSize = 0;
        
        for (String attributeName : attributeNames) {
            KafkaMessageColumnValue kafkaMessageColumnValue = record.getColumnValue(attributeName);

            if (kafkaMessageColumnValue == null || kafkaMessageColumnValue.getValue() == null) {
                // NULL is typically 4 bytes but since we don't deduct the '?' it's actually 3 bytes
                totalSize += 3;
            } else {
                Object value = kafkaMessageColumnValue.getValue();
                totalSize += estimateValueSize(value);
            }
        }

        return totalSize;
    }

    /**
     * Estimates the byte size of a value when serialized in SQL.
     * 
     * @param value the value to estimate
     * @return estimated byte size
     */
    private long estimateValueSize(Object value) {
        if (value instanceof String) {
            String str = (String) value;
            // Estimate: string bytes + escaped characters (rough estimate: 10% overhead)
            return (long)(str.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 1.1);
        }

        int length = String.valueOf(value).getBytes(StandardCharsets.UTF_8).length;
        if (value instanceof Number) {
            // Numbers are represented as strings in SQL
            return length;
        }

        if (value instanceof Boolean) {
            // Boolean: "true" or "false"
            return 5;
        }

        if (value instanceof byte[]) {
            // Byte arrays are typically hex-encoded: each byte becomes 2 hex chars
            return ((byte[]) value).length * 2L;
        }

        if (value instanceof java.util.Map) {
            // Maps are serialized as JSON strings
            try {
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(value);
                return json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            } catch (Exception e) {
                // Fallback: estimate based on toString()
                return length;
            }
        }

        // Fallback: estimate based on toString()
        return length;
    }
}
