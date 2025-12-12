package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Types;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemalessDateBinaryColumnDataTypeConverterTest {

    private final SchemalessDateBinaryColumnDataTypeConverter converter = new SchemalessDateBinaryColumnDataTypeConverter();
    private final TableSchema.Column dateColumn = new TableSchema.Column("d", "date", Types.DATE, false);

    @Test
    void convertsIsoStringToDaysSinceEpoch() {
        String s = "2025-01-02";
        Integer days = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(s), dateColumn);
        assertEquals((int) LocalDate.parse(s).toEpochDay(), days.intValue());
    }

    @Test
    void convertsNumberDays() {
        Integer days = converter.toParquetValue(new SchemalessKafkaMessageColumnValue(19700), dateColumn);
        assertEquals(19700, days.intValue());
    }

    @Test
    void invalidStringThrows() {
        assertThrows(ColumnConversionFailedException.class,
                () -> converter.toParquetValue(new SchemalessKafkaMessageColumnValue("2025-13-01"), dateColumn));
    }
}


