package com.firebolt.kafka.connect;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;

public class SchemalessKafkaMessageColumnValue extends KafkaMessageColumnValue {

    private static final Schema.Type NO_SCHEMA_TYPE = null;

    private static final Schema.Type NO_SCHEMA_SUB_TYPE = null;

    private static final Map<String, String> NO_SCHEMA_TYPE_PARAMS = null;

    public SchemalessKafkaMessageColumnValue(Object value) {
        super(value, NO_SCHEMA_TYPE, NO_SCHEMA_SUB_TYPE, NO_SCHEMA_TYPE_PARAMS);
    }
}
