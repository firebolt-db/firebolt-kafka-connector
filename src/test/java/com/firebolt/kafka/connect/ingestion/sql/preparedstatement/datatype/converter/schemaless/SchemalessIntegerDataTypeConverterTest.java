package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(42L).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 42);
    }

    @Test
    void parsesStringInteger() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(" 123 ").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 123);
    }

    @Test
    void invalidStringThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("abc").build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void convertsMinBoundaryLong() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value((long) Integer.MIN_VALUE).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, Integer.MIN_VALUE);
    }

    @Test
    void convertsMaxBoundaryLong() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value((long) Integer.MAX_VALUE).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, Integer.MAX_VALUE);
    }

    @Test
    void nonNumericTypeThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(12.34d).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void nullValueThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(null).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @ParameterizedTest
    @CsvSource({
            "2147483648",
            "-2147483649"
    })
    void outOfRangeLongThrows(long longValue) {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(longValue).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void acceptsByte() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value((byte) 10).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 10);
    }

    @Test
    void acceptsShort() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value((short) 100).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 100);
    }

    @Test
    void acceptsInteger() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(1000).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setInt(1, 1000);
    }
}


