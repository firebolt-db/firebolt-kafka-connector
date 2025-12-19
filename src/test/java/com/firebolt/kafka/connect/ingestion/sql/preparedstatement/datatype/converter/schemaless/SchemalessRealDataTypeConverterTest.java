package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
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

public class SchemalessRealDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessRealDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessRealDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "real", java.sql.Types.REAL, true);
    }

    @Test
    void convertsNumberToFloat() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(12.5d).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "12.5");
    }

    @Test
    void acceptsFloatByString() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("12.50").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "12.5");
    }

    @Test
    void invalidStringThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("abc").build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void acceptsFloatValueUsesString() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(Float.MAX_VALUE).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, String.valueOf(Float.MAX_VALUE));
    }

    @Test
    void convertsLongWithinFloatRange() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(100L).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setFloat(1, 100.0f);
    }

    @Test
    void nullValueThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(null).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }

    @Test
    void nonNumericTypeThrows() {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(true).build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }
}


