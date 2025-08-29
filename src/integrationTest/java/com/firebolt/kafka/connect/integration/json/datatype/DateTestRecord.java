package com.firebolt.kafka.connect.integration.json.datatype;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.DateListSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.DateSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.LocalDateSerializer;
import com.firebolt.kafka.connect.integration.json.datatype.serializer.LocalDateListSerializer;
import java.time.LocalDate;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * Test record for comprehensive Date serialization testing with JSON Schema and Kafka Connect.
 * 
 * This record tests:
 * - Required vs optional Date fields
 * - Date arrays with nullable and non-nullable elements
 * - Proper null handling for Date types
 * - JSON Schema validation for date formats
 * - End-to-end serialization from Kafka to Firebolt
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DateTestRecord {
    
    /**
     * Record identifier for test verification.
     */
    private Integer recordId;
    
    /**
     * Required date field - must not be null.
     * Maps to Firebolt DATE NOT NULL.
     */
    @JsonSerialize(using = DateSerializer.class)
    private Date requiredDate;

    /**
     * Required array where individual date elements can be null.
     * Maps to Firebolt ARRAY(DATE NULL) NOT NULL.
     */
    @JsonSerialize(using = DateListSerializer.class)
    private List<Date> requiredListWithNullableElements;

    /**
     * Required array where individual date elements cannot be null.
     * Maps to Firebolt ARRAY(DATE NOT NULL) NOT NULL.
     */
    @JsonSerialize(using = DateListSerializer.class)
    private List<Date> requiredListWithNonNullElements;

    /**
     * Optional date field - can be null or omitted.
     * Maps to Firebolt DATE NULL.
     */
    @JsonSerialize(using = DateSerializer.class)
    private Date optionalDate;

    /**
     * Optional array - entire array can be null/omitted, and elements can be null.
     * Maps to Firebolt ARRAY(DATE NULL) NULL.
     */
    @JsonSerialize(using = DateListSerializer.class)
    private List<Date> optionalList;

    /**
     * Optional array where individual date elements cannot be null.
     * Maps to Firebolt ARRAY(DATE NOT NULL) NULL.
     */
    @JsonSerialize(using = DateListSerializer.class)
    private List<Date> optionalListWithNonNullElements;

    @JsonSerialize(using = LocalDateListSerializer.class)
    private List<LocalDate> optionalLocalDateList;

    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate optionalLocalDate;

    private LocalDate localDateIso8601;

    private List<LocalDate> localDateIso8601List;

    // ISO-8601 serialized list of LocalDate (no custom serializer)
    // ISO-8601 serialized LocalDate (no custom serializer)
    // Optional date represented as ISO-8601 string (yyyy-MM-dd)
    private String dateAsString;

    // Optional list of ISO-8601 date strings (yyyy-MM-dd)
    private List<String> dateAsIso8601List;
}