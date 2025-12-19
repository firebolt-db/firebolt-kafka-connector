package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Interface that knows how to convert a value from kafka to a column in firebolt based on the column type
 */
public interface ColumnDataTypeConverter<T extends KafkaMessageColumnValue> {

    void convertAndSet(PreparedStatement statement, int paramIndex, T kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException, ColumnConversionFailedException;

}
