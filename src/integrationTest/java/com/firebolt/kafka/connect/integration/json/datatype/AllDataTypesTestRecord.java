package com.firebolt.kafka.connect.integration.json.datatype;


import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.DateListSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.DateSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.LocalDateTimeListSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.LocalDateTimeSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.OffsetDateTimeListSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.OffsetDateTimeSerializer;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


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
    @JsonSerialize(using = DateSerializer.class)
    private Date colDate;           // colDate DATE
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    private LocalDateTime colTimestamp;  // colTimestamp TIMESTAMP (epoch millis, Connect Timestamp logical)
    @JsonSerialize(using = OffsetDateTimeSerializer.class)
    private OffsetDateTime colTimestamptz; // colTimestamptz TIMESTAMPTZ
    
    // Binary type
    private String colBytea;             // colBytea BYTEA (base64 encoded string)
    
    // Array types
    private List<String> colArrayTextNullable;    // colArrayTextNullable ARRAY(TEXT NULL)
    private List<String> colArrayTextNotNull;     // colArrayTextNotNull ARRAY(TEXT NOT NULL)
    private List<Integer> colArrayIntSyntax1;     // colArrayIntSyntax1 ARRAY(INTEGER)
    private List<Integer> colArrayIntSyntax2;     // colArrayIntSyntax2 INTEGER[]

    @JsonSerialize(using = DateListSerializer.class)
    private List<Date> colArrayDate;         // colArrayDate ARRAY(DATE)
    private List<Float> colArrayReal;             // colArrayReal ARRAY(REAL)

    private List<BigDecimal> colArrayNumeric;     // colArrayNumeric ARRAY(NUMERIC)
    private List<Double> colArrayDoublePrecision; // colArrayDoublePrecision ARRAY(DOUBLE PRECISION)
    @JsonSerialize(using = OffsetDateTimeListSerializer.class)
    private List<OffsetDateTime> colArrayTimestamptz; // colArrayTimestamptz ARRAY(TIMESTAMPTZ)
    @JsonSerialize(using = LocalDateTimeListSerializer.class)
    private List<LocalDateTime> colArrayTimestamp;    // colArrayTimestamp ARRAY(TIMESTAMP) (epoch millis, Connect Timestamp logical)

} 