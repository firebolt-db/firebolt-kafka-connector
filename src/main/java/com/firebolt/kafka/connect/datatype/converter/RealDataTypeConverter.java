package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RealDataTypeConverter extends NumericDataTypeConverter {

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        // NOTE: the Float.MAX_VALUE will not be set in firebolt if we are using setFloat method, as it fails with the following error
        // Value of type double precision cannot be safely converted into type real. Need to do more investigation in phase 2
        statement.setString(paramIndex, String.valueOf((kafkaMessageColumnValue.getValue())));
    }

}
