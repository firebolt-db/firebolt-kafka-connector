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
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(12.5d).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setFloat(1, 12.5f);
    }

    @Test
    void acceptsFloatByString() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("12.50").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "12.5");
    }

    @Test
    void invalidStringThrows() {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("abc").build();
        assertThrows(ColumnConversionFailedException.class,
            () -> converter.convertAndSet(mockStatement, 1, value, testColumn));
    }
}


