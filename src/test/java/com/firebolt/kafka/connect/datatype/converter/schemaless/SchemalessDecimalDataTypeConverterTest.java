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

public class SchemalessDecimalDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessDecimalDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessDecimalDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "decimal", java.sql.Types.DECIMAL, true);
    }

    @Test
    void acceptsStringBigDecimal() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("123.450").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "123.450");
    }

    @Test
    void acceptsNumericToString() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(42).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "42");
    }

    @Test
    void invalidStringThrows() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("not-a-number").build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }
}


