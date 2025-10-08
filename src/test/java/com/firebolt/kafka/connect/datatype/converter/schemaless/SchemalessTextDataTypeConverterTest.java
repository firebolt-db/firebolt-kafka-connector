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

public class SchemalessTextDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessTextDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessTextDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "text", java.sql.Types.VARCHAR, true);
    }

    @Test
    void setsStringValue() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("hello").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "hello");
    }
}


