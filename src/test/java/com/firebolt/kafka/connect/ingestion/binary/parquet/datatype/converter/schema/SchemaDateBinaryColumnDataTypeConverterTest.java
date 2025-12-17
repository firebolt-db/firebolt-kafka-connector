package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaDateBinaryColumnDataTypeConverterTest {

    private final SchemaDateBinaryColumnDataTypeConverter converter = new SchemaDateBinaryColumnDataTypeConverter();
    private final TableSchema.Column dateColumn = new TableSchema.Column("d", "date", Types.DATE, false);

    @Test
    void convertsJavaUtilDateToEpochDays() {
        LocalDate ld = LocalDate.of(2023, 1, 2);
        Date utilDate = Date.from(ld.atStartOfDay(ZoneOffset.UTC).toInstant());
        Integer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT32)
                .value(utilDate)
                .build(), dateColumn);
        assertEquals((int) ld.toEpochDay(), result.intValue());
    }

    @ParameterizedTest
    @CsvSource({
            "2023-01-01",
            "1970-01-01",
            "2000-02-29",
            "2025-12-17"
    })
    void parsesIsoDateStringToEpochDays(String date) {
        Integer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.STRING)
                .value(date)
                .build(), dateColumn);
        assertEquals((int) LocalDate.parse(date).toEpochDay(), result.intValue());
    }

    @Test
    void convertsNumericDaysThrough() {
        Integer result = converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.INT32)
                .value(5)
                .build(), dateColumn);
        assertEquals(5, result.intValue());
    }

    @Test
    void invalidStringThrows() {
        ColumnConversionFailedException ex = assertThrows(ColumnConversionFailedException.class, () ->
                converter.toParquetValue(SchemaKafkaMessageColumnValue.builder()
                        .schemaType(Schema.Type.STRING)
                        .value("2023-1-02")
                        .build(), dateColumn)
        );
        assertEquals("d", ex.getColumnName());
        assertEquals("date", ex.getColumnType());
    }
}


