package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;

public class SchemalessDoubleDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessDoubleDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessDoubleDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "double", java.sql.Types.DOUBLE, true);
    }

    @Test
    void delegatesToParentDoubleConverter() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(12.34).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setDouble(1, 12.34);
    }
}


