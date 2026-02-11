package com.firebolt.kafka.connect.datatype;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents the supported firebolt data types that will be mapped to from the kafka connect message.  
 */
public enum FireboltColumnDataType {

    // have all the data types in lowercase
    INTEGER(value -> Set.of("integer", "int", "int4").contains(value.toLowerCase())),
    ARRAY(value -> value.toLowerCase().startsWith("array(")),
    TIMESTAMP(value -> Set.of("timestamp").contains(value.toLowerCase())),
    TIMESTAMPTZ(value -> Set.of("timestamptz").contains(value.toLowerCase())),
    DATE(value -> Set.of("date").contains(value.toLowerCase())),
    DECIMAL(value -> Set.of("numeric", "decimal").contains(value.toLowerCase())),
    BIGINT(value -> Set.of("bigint", "int8", "long").contains(value.toLowerCase())),
    REAL(value -> Set.of("real", "float4").contains(value.toLowerCase())),
    DOUBLE(value -> Set.of("double precision", "double", "float", "float8", "float(p)").contains(value.toLowerCase())),
    TEXT(value -> Set.of("text").contains(value.toLowerCase())),
    BYTEA(value -> Set.of("bytea").contains(value.toLowerCase())),
    BOOLEAN(value -> Set.of("boolean", "bool").contains(value.toLowerCase())),
    STRUCT(value -> Set.of("struct").contains(value.toLowerCase())),
    GEOGRAPHY(value -> Set.of("geography").contains(value.toLowerCase())),
    JSON(value -> Set.of("json").contains(value.toLowerCase()));

    private Predicate<String> equalityPredicate;

    FireboltColumnDataType(Predicate<String> equalityPredicate) {
        this.equalityPredicate = equalityPredicate;
    }

    public static FireboltColumnDataType fromString(String columnTypeName) {
        return Arrays.stream(FireboltColumnDataType.values())
                .filter(type -> type.equalityPredicate.test(columnTypeName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Invalid column type name: " + columnTypeName));
    }
}
