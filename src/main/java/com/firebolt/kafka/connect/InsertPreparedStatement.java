package com.firebolt.kafka.connect;

import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeFactoryProvider;
import com.firebolt.kafka.connect.datatype.converter.ColumnDataTypeConverter;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.datatype.converter.exception.RecordConversionFailedException;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

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
import org.apache.commons.collections.CollectionUtils;

/**
 * Insert into a table using prepared statements
 */
@Slf4j
public class InsertPreparedStatement implements IngestionService {

    protected Connection connection;
    private TableSchema tableSchema;
    private ErrorReporter errorReporter;
    private boolean errorToleranceAll;

    public InsertPreparedStatement(Connection connection, TableSchema tableSchema, ErrorReporter errorReporter, boolean errorToleranceAll) {
        this.connection = connection;
        this.tableSchema = tableSchema;
        this.errorReporter = errorReporter;
        this.errorToleranceAll = errorToleranceAll;
    }

    public void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        // since no schema evolution is supported, keep track of all the column names that are present in the records and filter out the ones that are not part of the table schema column names
        Set<String> columnNamesFromRecords = computeColumnNamesFromRecords(fireboltRecords);

        // detect all the columns will null values across all records
        Set<String> columnsWithNullValuesAcrossAllRecords = detectColumnsWithNullValuesAcrossAllRecords(fireboltRecords);

        Set<String> columnsWithAtLeastOneRecordWithNonNullValues = Sets.difference(columnNamesFromRecords, columnsWithNullValuesAcrossAllRecords);

        // map of firebolt column names to record attribute name
        Map<String, String> validColumnNames = filterValidColumns(columnsWithAtLeastOneRecordWithNonNullValues, tableSchema);

        if (CollectionUtils.isEmpty(validColumnNames.keySet())) {
            log.warn("There are not columns that have at least one non-null value and a matching column name in the table.");
            return;
        }

        // create the prepared statement and add the records
        addRecordsInternal(fireboltRecords, validColumnNames);
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close the connection");
            }
        }
    }


    private Set<String> detectColumnsWithNullValuesAcrossAllRecords(List<AbstractFireboltRecord> fireboltRecords) {
        Set<String> intersection = new HashSet<>(fireboltRecords.get(0).getColumnNamesWithNullValues());
        for (int i = 1; i < fireboltRecords.size(); i++) {
            intersection.retainAll(fireboltRecords.get(i).getColumnNamesWithNullValues());
        }
        return intersection;
    }

    private void addRecordsInternal(List<AbstractFireboltRecord> fireboltRecords, Map<String, String> validColumnNames) throws SQLException {
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
}
