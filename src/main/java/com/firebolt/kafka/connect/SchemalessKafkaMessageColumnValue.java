package com.firebolt.kafka.connect;

import lombok.experimental.SuperBuilder;

/**
 * Represents a column value from a kafka message without schema information
 */
@SuperBuilder
public class SchemalessKafkaMessageColumnValue implements KafkaMessageColumnValue {

    private Object value;

    public SchemalessKafkaMessageColumnValue(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

}
