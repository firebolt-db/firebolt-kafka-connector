package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Test record class for testing ARRAY data type serialization.
 * Uses List<Integer> for simple arrays and List<List<Integer>> for arrays of arrays.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ArrayTestRecord {

    private Integer recordId;

    // Simple array with nullable elements
    @JsonSerialize(using = IntegerListSerializer.class)
    private List<Integer> requiredArrayWithNullableElements;

    // Simple array with non-null elements
    @JsonSerialize(using = IntegerListSerializer.class)
    private List<Integer> requiredArrayWithNonNullElements;

    // Optional simple array
    @JsonSerialize(using = IntegerListSerializer.class)
    private List<Integer> optionalArray;

    // Optional simple array with non-null elements
    @JsonSerialize(using = IntegerListSerializer.class)
    private List<Integer> optionalArrayWithNonNullElements;

    // Array of arrays with nullable elements
    @JsonSerialize(using = ArrayOfArraysSerializer.class)
    private List<List<Integer>> requiredArrayOfArraysWithNullableElements;

    // Array of arrays with non-null elements
    @JsonSerialize(using = ArrayOfArraysSerializer.class)
    private List<List<Integer>> requiredArrayOfArraysWithNonNullElements;

    // Optional array of arrays
    @JsonSerialize(using = ArrayOfArraysSerializer.class)
    private List<List<Integer>> optionalArrayOfArrays;

    // Optional array of arrays with non-null elements
    @JsonSerialize(using = ArrayOfArraysSerializer.class)
    private List<List<Integer>> optionalArrayOfArraysWithNonNullElements;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArrayTestRecord that = (ArrayTestRecord) o;
        return Objects.equals(recordId, that.recordId) &&
               Objects.equals(requiredArrayWithNullableElements, that.requiredArrayWithNullableElements) &&
               Objects.equals(requiredArrayWithNonNullElements, that.requiredArrayWithNonNullElements) &&
               Objects.equals(optionalArray, that.optionalArray) &&
               Objects.equals(optionalArrayWithNonNullElements, that.optionalArrayWithNonNullElements) &&
               Objects.equals(requiredArrayOfArraysWithNullableElements, that.requiredArrayOfArraysWithNullableElements) &&
               Objects.equals(requiredArrayOfArraysWithNonNullElements, that.requiredArrayOfArraysWithNonNullElements) &&
               Objects.equals(optionalArrayOfArrays, that.optionalArrayOfArrays) &&
               Objects.equals(optionalArrayOfArraysWithNonNullElements, that.optionalArrayOfArraysWithNonNullElements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordId, requiredArrayWithNullableElements, requiredArrayWithNonNullElements,
                          optionalArray, optionalArrayWithNonNullElements,
                          requiredArrayOfArraysWithNullableElements, requiredArrayOfArraysWithNonNullElements,
                          optionalArrayOfArrays, optionalArrayOfArraysWithNonNullElements);
    }

    @Override
    public String toString() {
        return "ArrayTestRecord{" +
                "recordId=" + recordId +
                ", requiredArrayWithNullableElements=" + requiredArrayWithNullableElements +
                ", requiredArrayWithNonNullElements=" + requiredArrayWithNonNullElements +
                ", optionalArray=" + optionalArray +
                ", optionalArrayWithNonNullElements=" + optionalArrayWithNonNullElements +
                ", requiredArrayOfArraysWithNullableElements=" + requiredArrayOfArraysWithNullableElements +
                ", requiredArrayOfArraysWithNonNullElements=" + requiredArrayOfArraysWithNonNullElements +
                ", optionalArrayOfArrays=" + optionalArrayOfArrays +
                ", optionalArrayOfArraysWithNonNullElements=" + optionalArrayOfArraysWithNonNullElements +
                '}';
    }

    /**
     * Custom serializer for Integer lists to ensure proper JSON formatting.
     */
    public static class IntegerListSerializer extends JsonSerializer<List<Integer>> {
        @Override
        public void serialize(List<Integer> value, JsonGenerator gen, SerializerProvider serializers) throws java.io.IOException {
            if (value == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                for (Integer item : value) {
                    if (item == null) {
                        gen.writeNull();
                    } else {
                        gen.writeNumber(item);
                    }
                }
                gen.writeEndArray();
            }
        }
    }

    /**
     * Custom serializer for arrays of arrays to ensure proper JSON formatting.
     */
    public static class ArrayOfArraysSerializer extends JsonSerializer<List<List<Integer>>> {
        @Override
        public void serialize(List<List<Integer>> value, JsonGenerator gen, SerializerProvider serializers) throws java.io.IOException {
            if (value == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                for (List<Integer> innerArray : value) {
                    if (innerArray == null) {
                        gen.writeNull();
                    } else {
                        gen.writeStartArray();
                        for (Integer item : innerArray) {
                            if (item == null) {
                                gen.writeNull();
                            } else {
                                gen.writeNumber(item);
                            }
                        }
                        gen.writeEndArray();
                    }
                }
                gen.writeEndArray();
            }
        }
    }
} 