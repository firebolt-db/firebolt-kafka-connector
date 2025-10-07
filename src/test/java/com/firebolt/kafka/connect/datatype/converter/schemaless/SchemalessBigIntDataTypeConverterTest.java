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

public class SchemalessBigIntDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessBigIntDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessBigIntDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "bigint", java.sql.Types.BIGINT, true);
    }

    @Test
    void setsLongValue() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(123L).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setLong(1, 123L);
    }
}


