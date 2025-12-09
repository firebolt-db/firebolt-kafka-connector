package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import java.sql.Types;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemalessArrayColumnDateDataTypeConverterTest {

    private final SchemalessArrayColumnDataTypeConverter arrayConverter = new SchemalessArrayColumnDataTypeConverter();
    private final SchemalessDateColumnDataTypeConverter dateConverter = new SchemalessDateColumnDataTypeConverter();
    private final TableSchema.Column arrayDateColumn = new TableSchema.Column("ds", "array(date)", Types.ARRAY, false);

    @Test
    void convertsMixedElementsToDaysSinceEpoch() {
        arrayConverter.addConverter(FireboltColumnDataType.DATE, dateConverter);
        String s = "2025-01-02";
        int days = (int) LocalDate.parse(s).toEpochDay();
        List<Object> input = Arrays.asList(s, 19700, null);
        List<?> result = arrayConverter.toParquetValue(new SchemalessKafkaMessageColumnValue(input), arrayDateColumn);
        assertEquals(Arrays.asList(days, 19700, null), result);
    }
}


