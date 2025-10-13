package com.firebolt.kafka.connect.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.AbstractColumnTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaBooleanDataTypeConverter extends AbstractColumnTypeConverter<SchemaKafkaMessageColumnValue> {

    private static Set<String> ALLOWED_STRING_VALUES_AS_BOOLEAN = Set.of("t", "f", "true", "false", "0", "1");

    @Override
    public void convertAndSet(PreparedStatement statement, int paramIndex, SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws SQLException {
        if (schemaKafkaMessageColumnValue.getValue() instanceof Boolean) {
            statement.setBoolean(paramIndex, (Boolean) schemaKafkaMessageColumnValue.getValue());
            return;
        }

        if (schemaKafkaMessageColumnValue.getValue() instanceof String) {
            String kafkaValue = (String) schemaKafkaMessageColumnValue.getValue();
            if (isValidBooleanValueAsString(kafkaValue)) {
                log.debug("Setting the string value {} as a boolean", kafkaValue);
                statement.setString(paramIndex, kafkaValue);
                return;
            }
        }

        throw aColumnConversionFailedException(fireboltColumn, schemaKafkaMessageColumnValue.getValue());
    }

    private boolean isValidBooleanValueAsString(String valueFromKafkaMessage) {
        return ALLOWED_STRING_VALUES_AS_BOOLEAN.contains(valueFromKafkaMessage.toLowerCase());
    }
}


