package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Custom serializer for List<BigDecimal> to ensure each element is serialized as a string.
 */
class NumericBigDecimalListSerializer extends JsonSerializer<List<BigDecimal>> {
    @Override
    public void serialize(List<BigDecimal> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        
        gen.writeStartArray();
        for (BigDecimal decimal : value) {
            if (decimal == null) {
                gen.writeNull();
            } else {
                gen.writeString(decimal.toString());
            }
        }
        gen.writeEndArray();
    }
}

/**
 * Test record class for testing NUMERIC data type serialization.
 * Uses BigDecimal for precise decimal arithmetic and follows the same pattern as other test records.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NumericTestRecord {
    
    private Integer recordId;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal requiredNumeric;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal optionalNumeric;
    
    @JsonSerialize(using = NumericBigDecimalListSerializer.class)
    private List<BigDecimal> requiredListWithNullableElements;
    
    @JsonSerialize(using = NumericBigDecimalListSerializer.class)
    private List<BigDecimal> requiredListWithNonNullElements;
    
    @JsonSerialize(using = NumericBigDecimalListSerializer.class)
    private List<BigDecimal> optionalList;
    
    @JsonSerialize(using = NumericBigDecimalListSerializer.class)
    private List<BigDecimal> optionalListWithNonNullElements;

} 