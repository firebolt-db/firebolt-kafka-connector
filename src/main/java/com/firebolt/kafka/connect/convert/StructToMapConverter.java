package com.firebolt.kafka.connect.convert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Converts Kafka Connect Struct to Map for JSON serialization.
 * Used when ingesting Avro/JSON Schema records into Firebolt JSON columns.
 * Handles nested Structs, Lists, and Maps recursively.
 */
public final class StructToMapConverter {

    private StructToMapConverter() {
    }

    public static Map<String, Object> convert(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : struct.schema().fields()) {
            Object fieldValue = struct.get(field);
            map.put(field.name(), convertValue(fieldValue, field.schema()));
        }
        return map;
    }

    private static Object convertValue(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        if (value instanceof Struct) {
            return convert((Struct) value);
        }
        if (schema != null && schema.type() == Schema.Type.ARRAY && value instanceof List) {
            Schema valueSchema = schema.valueSchema();
            List<Object> converted = new ArrayList<>();
            for (Object item : (List<?>) value) {
                converted.add(convertValue(item, valueSchema));
            }
            return converted;
        }
        if (schema != null && schema.type() == Schema.Type.MAP && value instanceof Map) {
            Schema valueSchema = schema.valueSchema();
            Map<String, Object> converted = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                converted.put(String.valueOf(entry.getKey()),
                        convertValue(entry.getValue(), valueSchema));
            }
            return converted;
        }
        return value;
    }
}
