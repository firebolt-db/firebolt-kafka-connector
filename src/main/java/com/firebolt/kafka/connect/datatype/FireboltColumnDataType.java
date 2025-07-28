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
    ARRAY(value -> value.toLowerCase().startsWith("array("));

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
