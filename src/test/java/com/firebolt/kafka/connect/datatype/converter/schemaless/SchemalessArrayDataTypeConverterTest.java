package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class SchemalessArrayDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;
    @Mock
    private Connection mockConnection;
    @Mock
    private Array mockArray;

    private SchemalessArrayDataTypeConverter converter;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessArrayDataTypeConverter();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
    }

    @Test
    void createsIntegerArray() throws SQLException {
        TableSchema.Column col = new TableSchema.Column("ints", "array(integer)", 2003, true);
        List<Object> list = Arrays.asList(1, 2, 3);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();
        when(mockConnection.createArrayOf("integer", list.toArray())).thenReturn(mockArray);

        converter.convertAndSet(mockStatement, 1, value, col);
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    void convertsTimestampLongsToStringArray() throws SQLException {
        TableSchema.Column col = new TableSchema.Column("ts", "array(timestamp)", 2003, true);
        List<Object> list = Arrays.asList(1705336245000L, null, 1705336245123L);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();

        ArgumentCaptor<Object[]> elementsCaptor = ArgumentCaptor.forClass(Object[].class);
        when(mockConnection.createArrayOf(org.mockito.ArgumentMatchers.eq("string"), elementsCaptor.capture())).thenReturn(mockArray);

        converter.convertAndSet(mockStatement, 1, value, col);
        verify(mockStatement).setArray(1, mockArray);
        Object[] created = elementsCaptor.getValue();
        // Use TimestampUtil formatting to avoid JDK-specific variations
        String expected0 = com.firebolt.kafka.connect.datatype.converter.TimestampUtil.asTimestamp(1705336245000L).toInstant().toString();
        assertEquals(expected0, created[0]);
        assertEquals(null, created[1]);
    }

    @Test
    void createsRealArrayFromMixedNumericTypes() throws SQLException {
        TableSchema.Column col = new TableSchema.Column("reals", "array(real)", 2003, true);
        List<Object> list = Arrays.asList(100L, 12.5d, null);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();

        ArgumentCaptor<Object[]> elementsCaptor = ArgumentCaptor.forClass(Object[].class);
        when(mockConnection.createArrayOf(org.mockito.ArgumentMatchers.eq("string"), elementsCaptor.capture())).thenReturn(mockArray);

        converter.convertAndSet(mockStatement, 1, value, col);
        verify(mockStatement).setArray(1, mockArray);

        Object[] created = elementsCaptor.getValue();
        assertEquals("100", created[0]);
        assertEquals("12.5", created[1]);
        assertEquals(null, created[2]);
    }

    @Test
    void realArrayWithInvalidStringThrows() {
        TableSchema.Column col = new TableSchema.Column("reals", "array(real)", 2003, true);
        List<Object> list = Arrays.asList("abc");
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, value, col));
    }

    @Test
    void realArrayWithUnsupportedTypeThrows() {
        TableSchema.Column col = new TableSchema.Column("reals", "array(real)", 2003, true);
        List<Object> list = Arrays.asList(true);
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();

        assertThrows(ColumnConversionFailedException.class, () ->
                converter.convertAndSet(mockStatement, 1, value, col));
    }

    @Test
    void realArrayEmptyListCreatesRealArrayType() throws SQLException {
        TableSchema.Column col = new TableSchema.Column("reals", "array(real)", 2003, true);
        List<Object> list = Arrays.asList();
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(list).build();

        when(mockConnection.createArrayOf("real", list.toArray())).thenReturn(mockArray);

        converter.convertAndSet(mockStatement, 1, value, col);
        verify(mockStatement).setArray(1, mockArray);
    }
}


