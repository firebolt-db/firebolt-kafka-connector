package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A class that tries to convert the value from the kafka message to an integer value
 */
public class IntegerDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        statement.setInt(paramIndex, asInteger(kafkaMessageColumnValue));
    }

    private int asInteger(KafkaMessageColumnValue value) {
        // this is a known limitation of JsonSchemaConverter from io.confluent that treats all integer types as int64, akd longs
        return ((Long) value.getValue()).intValue();
    }

}
