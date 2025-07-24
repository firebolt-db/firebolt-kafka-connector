package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Custom serializer for List<BigDecimal> to ensure each element is serialized as a string.
 */
class BigDecimalListSerializer extends JsonSerializer<List<BigDecimal>> {
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
 * Test record class that mirrors the structure of the all data types test table.
 * This class provides a Java object representation of all Firebolt data types
 * for use in integration tests.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AllDataTypesTestRecord {
    
    // Numeric types
    private Integer colInteger;           // colInteger INTEGER NOT NULL
    private Long colBigint;              // colBigint BIGINT

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
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
    private List<List<Integer>> colArrayNested;   // colArrayNested ARRAY(ARRAY(INTEGER))
    
    @JsonSerialize(using = BigDecimalListSerializer.class)
    private List<BigDecimal> colArrayNumeric;     // colArrayNumeric ARRAY(NUMERIC)
    private List<Double> colArrayDoublePrecision; // colArrayDoublePrecision ARRAY(DOUBLE PRECISION)
    private List<OffsetDateTime> colArrayTimestamptz; // colArrayTimestamptz ARRAY(TIMESTAMPTZ)
    private List<LocalDateTime> colArrayTimestamp;    // colArrayTimestamp ARRAY(TIMESTAMP)
    
    // STRUCT type  
    private TestStruct colStruct;                     // colStruct STRUCT(name TEXT, age INTEGER, active BOOLEAN, score DOUBLE PRECISION)
    
    // GEOGRAPHY type
    // private String colGeography;                  // colGeography GEOGRAPHY
    
    // Methods for object comparison and string representation
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllDataTypesTestRecord that = (AllDataTypesTestRecord) o;
        return Objects.equals(colInteger, that.colInteger) &&
               Objects.equals(colBigint, that.colBigint) &&
               Objects.equals(colNumeric, that.colNumeric) &&
               Objects.equals(colReal, that.colReal) &&
               Objects.equals(colDoublePrecision, that.colDoublePrecision) &&
               Objects.equals(colBoolean, that.colBoolean) &&
               Objects.equals(colText, that.colText) &&
               Objects.equals(colDate, that.colDate) &&
               Objects.equals(colTimestamp, that.colTimestamp) &&
               Objects.equals(colTimestamptz, that.colTimestamptz) &&
               Objects.equals(colBytea, that.colBytea) &&
               Objects.equals(colArrayTextNullable, that.colArrayTextNullable) &&
               Objects.equals(colArrayTextNotNull, that.colArrayTextNotNull) &&
               Objects.equals(colArrayIntSyntax1, that.colArrayIntSyntax1) &&
               Objects.equals(colArrayIntSyntax2, that.colArrayIntSyntax2) &&
               Objects.equals(colArrayDate, that.colArrayDate) &&
               Objects.equals(colArrayReal, that.colArrayReal) &&
               Objects.equals(colArrayNested, that.colArrayNested) &&
               Objects.equals(colArrayNumeric, that.colArrayNumeric) &&
               Objects.equals(colArrayDoublePrecision, that.colArrayDoublePrecision) &&
               Objects.equals(colArrayTimestamptz, that.colArrayTimestamptz) &&
               Objects.equals(colArrayTimestamp, that.colArrayTimestamp) &&
               Objects.equals(colStruct, that.colStruct);
               // Objects.equals(colGeography, that.colGeography);
    }
    
    @Override
    public int hashCode() {
        int result = Objects.hash(colInteger, colBigint, colNumeric, colReal, colDoublePrecision, colBoolean, colText, colDate, colTimestamp, colTimestamptz, colBytea, colArrayTextNullable, colArrayTextNotNull, colArrayIntSyntax1, colArrayIntSyntax2, colArrayDate, colArrayReal, colArrayNested, colArrayNumeric, colArrayDoublePrecision, colArrayTimestamptz, colArrayTimestamp, colStruct);
        return result;
               // colArrayTextNotNull, colArrayIntSyntax1, colArrayIntSyntax2, 
               // colArrayDate, colArrayReal, colArrayNested, colStruct, colGeography);
    }
    
    @Override
    public String toString() {
        return "AllDataTypesTestRecord{" +
               "colInteger=" + colInteger +
               ", colBigint=" + colBigint +
               ", colNumeric=" + colNumeric +
               ", colReal=" + colReal +
               ", colDoublePrecision=" + colDoublePrecision +
               ", colBoolean=" + colBoolean +
               ", colText='" + colText + '\'' +
               ", colDate=" + colDate +
               ", colTimestamp=" + colTimestamp +
               ", colTimestamptz=" + colTimestamptz +
               ", colBytea=" + colBytea +
               ", colArrayTextNullable=" + colArrayTextNullable +
               ", colArrayTextNotNull=" + colArrayTextNotNull +
               ", colArrayIntSyntax1=" + colArrayIntSyntax1 +
               ", colArrayIntSyntax2=" + colArrayIntSyntax2 +
               ", colArrayDate=" + colArrayDate +
               ", colArrayReal=" + colArrayReal +
               ", colArrayNested=" + colArrayNested +
               ", colArrayNumeric=" + colArrayNumeric +
               ", colArrayDoublePrecision=" + colArrayDoublePrecision +
               ", colArrayTimestamptz=" + colArrayTimestamptz +
               ", colArrayTimestamp=" + colArrayTimestamp +
               ", colStruct=" + colStruct +
               // ", colGeography='" + colGeography + '\'' +
               '}';
    }
} 