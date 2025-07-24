package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Test record class for testing DOUBLE PRECISION data type serialization.
 * Uses Double for floating-point arithmetic with 15 decimal-digit precision.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DoubleTestRecord {
    
    private Integer recordId;
    
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Double requiredDouble;
    
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Double optionalDouble;
    
    @JsonSerialize(using = DoubleListSerializer.class)
    private List<Double> requiredListWithNullableElements;
    
    @JsonSerialize(using = DoubleListSerializer.class)
    private List<Double> requiredListWithNonNullElements;
    
    @JsonSerialize(using = DoubleListSerializer.class)
    private List<Double> optionalList;
    
    @JsonSerialize(using = DoubleListSerializer.class)
    private List<Double> optionalListWithNonNullElements;
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoubleTestRecord that = (DoubleTestRecord) o;
        return Objects.equals(recordId, that.recordId) &&
               Objects.equals(requiredDouble, that.requiredDouble) &&
               Objects.equals(optionalDouble, that.optionalDouble) &&
               Objects.equals(requiredListWithNullableElements, that.requiredListWithNullableElements) &&
               Objects.equals(requiredListWithNonNullElements, that.requiredListWithNonNullElements) &&
               Objects.equals(optionalList, that.optionalList) &&
               Objects.equals(optionalListWithNonNullElements, that.optionalListWithNonNullElements);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(recordId, requiredDouble, optionalDouble, 
                          requiredListWithNullableElements, requiredListWithNonNullElements,
                          optionalList, optionalListWithNonNullElements);
    }
    
    @Override
    public String toString() {
        return "DoubleTestRecord{" +
                "recordId=" + recordId +
                ", requiredDouble=" + requiredDouble +
                ", optionalDouble=" + optionalDouble +
                ", requiredListWithNullableElements=" + requiredListWithNullableElements +
                ", requiredListWithNonNullElements=" + requiredListWithNonNullElements +
                ", optionalList=" + optionalList +
                ", optionalListWithNonNullElements=" + optionalListWithNonNullElements +
                '}';
    }
    
    /**
     * Custom serializer for Double lists to ensure proper JSON formatting.
     */
    public static class DoubleListSerializer extends JsonSerializer<List<Double>> {
        @Override
        public void serialize(List<Double> value, JsonGenerator gen, SerializerProvider serializers) throws java.io.IOException {
            if (value == null) {
                gen.writeNull();
            } else {
                gen.writeStartArray();
                for (Double item : value) {
                    if (item == null) {
                        gen.writeNull();
                    } else {
                        gen.writeString(item.toString());
                    }
                }
                gen.writeEndArray();
            }
        }
    }
} 