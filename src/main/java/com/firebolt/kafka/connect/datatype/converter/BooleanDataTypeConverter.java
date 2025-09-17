package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BooleanDataTypeConverter implements ColumnDataTypeConverter {

    // keep them as lowercase
    private static Set<String> ALLOWED_STRING_VALUES_AS_BOOLEAN = Set.of("t", "f", "true", "false", "0", "1");

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        // we can set boolean value from multiple object types so go over these types one by one
        if (kafkaMessageColumnValue.getValue() instanceof Boolean) {
            statement.setBoolean(paramIndex, (Boolean) kafkaMessageColumnValue.getValue());
            return;
        }

        if (kafkaMessageColumnValue.getValue() instanceof String) {
            String kafkaValue = (String) kafkaMessageColumnValue.getValue();
            if (isValidBooleanValueAsString(kafkaValue)) {
                log.debug("Setting the string value {} as a boolean", kafkaValue);
                statement.setString(paramIndex, kafkaValue);
                return;
            }
        }

        throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(), "Cannot convert kafka message attribute to a boolean due to incompatible type: " + (kafkaMessageColumnValue.getValue() != null ? kafkaMessageColumnValue.getValue().getClass().getName() : "null"));
    }

    private boolean isValidBooleanValueAsString(String valueFromKafkaMessage) {
        return ALLOWED_STRING_VALUES_AS_BOOLEAN.contains(valueFromKafkaMessage.toLowerCase());
    }

}
