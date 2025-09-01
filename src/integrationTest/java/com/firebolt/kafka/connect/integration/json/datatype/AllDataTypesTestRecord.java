package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Custom serializer for OffsetDateTime to convert to timestamp value (microseconds since epoch).
 * This ensures compatibility with Kafka Connect Timestamp logical type.
 */
class OffsetDateTimeSerializer extends JsonSerializer<OffsetDateTime> {
    @Override
    public void serialize(OffsetDateTime value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
        } else {
            // Convert to microseconds since epoch
            long timestampMicros = value.toInstant().getEpochSecond() * 1_000_000 + value.toInstant().getNano() / 1_000;
            gen.writeNumber(timestampMicros);
        }
    }
}

/**
 * Custom serializer for List<OffsetDateTime> to convert each element to timestamp values.
 */
class OffsetDateTimeListSerializer extends JsonSerializer<List<OffsetDateTime>> {
    @Override
    public void serialize(List<OffsetDateTime> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        
        gen.writeStartArray();
        for (OffsetDateTime dateTime : value) {
            if (dateTime == null) {
                gen.writeNull();
            } else {
                // Convert to microseconds since epoch
                long timestampMicros = dateTime.toInstant().getEpochSecond() * 1_000_000 + dateTime.toInstant().getNano() / 1_000;
                gen.writeNumber(timestampMicros);
            }
        }
        gen.writeEndArray();
    }
}

/**
 * Test record class that mirrors the structure of the all data types test table.
 * This class provides a Java object representation of all Firebolt data types
 * for use in integration tests.
 */
@Data
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AllDataTypesTestRecord {
    
    // Numeric types
    private Integer colInteger;           // colInteger INTEGER NOT NULL
    private Long colBigint;              // colBigint BIGINT

    private BigDecimal colNumeric;       // colNumeric NUMERIC(38,9) - serialized as string to preserve precision
    private Float colReal;               // colReal REAL
    private Double colDoublePrecision;   // colDoublePrecision DOUBLE PRECISION
    
    // Boolean type
    private Boolean colBoolean;          // colBoolean BOOLEAN
    
    // String type
    private String colText;              // colText TEXT
    
    // Date and timestamp types
    private LocalDate colDate;           // colDate DATE
    private LocalDateTime colTimestamp;  // colTimestamp TIMESTAMP
    @JsonSerialize(using = OffsetDateTimeSerializer.class)
    private OffsetDateTime colTimestamptz; // colTimestamptz TIMESTAMPTZ
    
    // Binary type
    private String colBytea;             // colBytea BYTEA (base64 encoded string)
    
    // Array types
    private List<String> colArrayTextNullable;    // colArrayTextNullable ARRAY(TEXT NULL)
    private List<String> colArrayTextNotNull;     // colArrayTextNotNull ARRAY(TEXT NOT NULL)
    private List<Integer> colArrayIntSyntax1;     // colArrayIntSyntax1 ARRAY(INTEGER)
    private List<Integer> colArrayIntSyntax2;     // colArrayIntSyntax2 INTEGER[]
    private List<LocalDate> colArrayDate;         // colArrayDate ARRAY(DATE)
    private List<Float> colArrayReal;             // colArrayReal ARRAY(REAL)

    private List<BigDecimal> colArrayNumeric;     // colArrayNumeric ARRAY(NUMERIC)
    private List<Double> colArrayDoublePrecision; // colArrayDoublePrecision ARRAY(DOUBLE PRECISION)
    @JsonSerialize(using = OffsetDateTimeListSerializer.class)
    private List<OffsetDateTime> colArrayTimestamptz; // colArrayTimestamptz ARRAY(TIMESTAMPTZ)
    private List<LocalDateTime> colArrayTimestamp;    // colArrayTimestamp ARRAY(TIMESTAMP)

} 