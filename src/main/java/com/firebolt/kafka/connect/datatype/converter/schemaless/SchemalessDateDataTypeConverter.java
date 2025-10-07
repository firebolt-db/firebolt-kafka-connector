package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.DateDataTypeConverter;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class SchemalessDateDataTypeConverter extends DateDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = kafkaMessageColumnValue.getValue();

        // if it is a number then it is considered to be the number of seconds from epoch
        if (value instanceof Number) {
            int numberOfDaysFromEpoch = ((Number) value).intValue();
            LocalDate localDate = LocalDate.ofEpochDay(numberOfDaysFromEpoch);
            statement.setDate(paramIndex, Date.valueOf(localDate));
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


