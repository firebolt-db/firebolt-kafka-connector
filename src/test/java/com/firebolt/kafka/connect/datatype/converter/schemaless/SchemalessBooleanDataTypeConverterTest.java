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

public class SchemalessBooleanDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessBooleanDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessBooleanDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "boolean", java.sql.Types.BOOLEAN, true);
    }

    @Test
    void setsBooleanValue() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(true).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setBoolean(1, true);
    }
}


