package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaBooleanDataTypeConverter extends AbstractColumnTypeConverter {

    private static Set<String> ALLOWED_STRING_VALUES_AS_BOOLEAN = Set.of("t", "f", "true", "false", "0", "1");

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, KafkaMessageColumnValue kafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
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

        throw aColumnConversionFailedException(fireboltColumn, kafkaMessageColumnValue.getValue());
    }

    private boolean isValidBooleanValueAsString(String valueFromKafkaMessage) {
        return ALLOWED_STRING_VALUES_AS_BOOLEAN.contains(valueFromKafkaMessage.toLowerCase());
    }
}


