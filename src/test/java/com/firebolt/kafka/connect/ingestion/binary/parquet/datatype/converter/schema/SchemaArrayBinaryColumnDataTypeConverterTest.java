package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.BinaryColumnDataTypeConverter;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class SchemaArrayBinaryColumnDataTypeConverterTest {

    @Test
    void convertsIntegerArrayElements() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();

        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> intConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.INTEGER, intConverter);

        TableSchema.Column col = new TableSchema.Column("ints", "array(integer)", Types.ARRAY, true);

        List<Object> elements = Arrays.asList(1, 2L, "3", null);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT32)
                .schemaTypeParams(Collections.emptyMap())
                .value(elements)
                .build();

        when(intConverter.toParquetValue(any(SchemaKafkaMessageColumnValue.class), eq(col)))
                .thenAnswer(inv -> {
                    Object v = ((SchemaKafkaMessageColumnValue) inv.getArgument(0)).getValue();
                    if (v instanceof Number) {
                        return ((Number) v).intValue();
                    }
                    return Integer.parseInt(v.toString());
                });

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertEquals(Arrays.asList(1, 2, 3, null), result);

        ArgumentCaptor<SchemaKafkaMessageColumnValue> captor = ArgumentCaptor.forClass(SchemaKafkaMessageColumnValue.class);
        verify(intConverter, times(3)).toParquetValue(captor.capture(), eq(col));
        List<SchemaKafkaMessageColumnValue> captured = captor.getAllValues();
        assertEquals(3, captured.size());
        assertEquals(1, captured.get(0).getValue());
        assertEquals(2L, captured.get(1).getValue());
        assertEquals("3", captured.get(2).getValue());
        assertTrue(captured.stream().allMatch(v -> v.getSchemaType() == Schema.Type.ARRAY && v.getSchemaSubType() == Schema.Type.INT32));
    }

    @Test
    void returnsEmptyListWhenElementsEmpty() {
        SchemaArrayBinaryColumnDataTypeConverter arrayConverter = new SchemaArrayBinaryColumnDataTypeConverter();
        BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer> intConverter =
                (BinaryColumnDataTypeConverter<SchemaKafkaMessageColumnValue, Integer>) mock(BinaryColumnDataTypeConverter.class);
        arrayConverter.addConverter(FireboltColumnDataType.INTEGER, intConverter);

        TableSchema.Column col = new TableSchema.Column("ints", "array(integer)", Types.ARRAY, true);
        SchemaKafkaMessageColumnValue arrayValue = SchemaKafkaMessageColumnValue.builder()
                .schemaType(Schema.Type.ARRAY)
                .schemaSubType(Schema.Type.INT32)
                .schemaTypeParams(Map.of())
                .value(List.of())
                .build();

        List<? extends Object> result = arrayConverter.toParquetValue(arrayValue, col);
        assertTrue(result.isEmpty());
        verifyNoInteractions(intConverter);
    }


}

