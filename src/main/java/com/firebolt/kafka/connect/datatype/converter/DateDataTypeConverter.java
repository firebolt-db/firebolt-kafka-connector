package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DateDataTypeConverter extends AbstractColumnTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();
        if (value instanceof Date) {
            Date date = (Date) kafkaMessageColumnValue.getValue();
            statement.setDate(paramIndex, new java.sql.Date(date.getTime()));
            return;
        }

        if (value instanceof String) {
            if (isIsoLocalDate((String) value)) {
                statement.setString(paramIndex, (String) value);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

}
