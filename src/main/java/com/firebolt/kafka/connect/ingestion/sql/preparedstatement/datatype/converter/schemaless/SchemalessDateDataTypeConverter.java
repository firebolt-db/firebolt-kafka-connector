package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.AbstractColumnTypeConverter;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class SchemalessDateDataTypeConverter extends AbstractColumnTypeConverter<SchemalessKafkaMessageColumnValue> {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

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


