package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class SchemalessIntegerDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessIntegerDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessIntegerDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "integer", java.sql.Types.INTEGER, true);
    }

    @Test
    void convertsNumberToInteger() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(42L).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 42);
    }

    @Test
    void parsesStringInteger() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(" 123 ").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 123);
    }

    @Test
    void invalidStringThrows() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("abc").build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void convertsMinBoundaryLong() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value((long) Integer.MIN_VALUE).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, Integer.MIN_VALUE);
    }

    @Test
    void convertsMaxBoundaryLong() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value((long) Integer.MAX_VALUE).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, Integer.MAX_VALUE);
    }

    @Test
    void nonNumericTypeThrows() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(12.34d).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void nullValueThrows() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(null).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }
}


