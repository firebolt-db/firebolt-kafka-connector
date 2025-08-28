package com.firebolt.kafka.connect;

import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverterFactory;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
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
import lombok.extern.slf4j.Slf4j;

/**
 * Insert into a table using prepared statements
 */
@Slf4j
public class InsertPreparedStatement {

    private Connection connection;
    private TableSchema tableSchema;

    public InsertPreparedStatement(Connection connection, TableSchema tableSchema) {
        this.connection = connection;
        this.tableSchema = tableSchema;
    }

    public void addRecords(List<FireboltRecord> fireboltRecords) throws SQLException {
        // since no schema evolution is supported, keep track of all the column names that are present in the records and filter out the ones that are not part of the table schema column names
        Set<String> columnNamesFromRecords = computeColumnNamesFromRecords(fireboltRecords);

        // map of firebolt column names to record attribute name
        Map<String, String> validColumnNames = filterValidColumns(columnNamesFromRecords, tableSchema);

        // create the prepared statement and add the records
        addRecordsInternal(fireboltRecords, validColumnNames);
    }

    private void addRecordsInternal(List<FireboltRecord> fireboltRecords, Map<String, String> validColumnNames) throws SQLException {
        try (PreparedStatement preparedStatement = createPreparedStatement(tableSchema, validColumnNames.keySet())) {

            // Execute batch insert
            for (FireboltRecord record : fireboltRecords) {
                try {
                    setStatementParameters(preparedStatement, record, tableSchema, validColumnNames);
                    preparedStatement.addBatch();
                } catch (RecordConversionFailedException e) {
                    // send the record to the dead letter queue
                    log.warn("Record from partition {} at offset {} will be submitted to the deadletter queue ", e.getKafkaPartition(), e.getKafkaOffset());
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
                    FireboltRecord fireboltRecord = fireboltRecords.get(0);
                    log.warn("Cannot process the firebolt record from partition {} at offset {}, as it is too large and exceeds the Firebolt request entity size", fireboltRecord.getPartition(), fireboltRecord.getOffset());

                    // TODO should add this to the dead letter queue
                } else {
                    // simple strategy for now. Split the rows in two and try each half again
                    int leftSide = fireboltRecords.size() / 2;
                    log.warn("Batch size was too big so will split the records in smaller batches {} & {}", leftSide, fireboltRecords.size()-leftSide);
                    addRecordsInternal(fireboltRecords.subList(0,leftSide), validColumnNames);
                    addRecordsInternal(fireboltRecords.subList(leftSide,fireboltRecords.size()), validColumnNames);
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

    private void setStatementParameters(PreparedStatement stmt, FireboltRecord record, TableSchema schema, Map<String, String> validColumnNames) throws SQLException, RecordConversionFailedException {
        int parameterIndex = 1;
        for (TableSchema.Column column : schema.getColumns()) {
            if (validColumnNames.containsKey(column.getName())) {
                String attributeName = validColumnNames.get(column.getName());
                KafkaMessageColumnValue value = record.getColumnValues().get(attributeName);

                if (value == null || value.getValue() == null) {
                    stmt.setNull(parameterIndex, column.getSqlType());
                } else {
                    ColumnDataTypeConverter columnDataTypeConverter = ColumnDataTypeConverterFactory.getInstance().getConverter(column);
                    try {
                        columnDataTypeConverter.convertAndSet(stmt, parameterIndex, value, column);
                    } catch (ColumnConversionFailedException e) {
                        log.error("Conversion failed for table {} column {} for kafka record from partition {} offset {}", schema.getTableName(), column.getName(), record.getPartition(), record.getOffset());

                        // as of now we are failing at the first column conversion failure. We could try to convert all the columns so we give all the data in one record convertion exception.
                        throw RecordConversionFailedException.builder()
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
}
