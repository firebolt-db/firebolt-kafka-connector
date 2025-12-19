package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessByteaDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;

public class SchemalessByteaDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessByteaDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessByteaDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "bytea", java.sql.Types.BINARY, true);
    }

    @Test
    void acceptsString() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("aGVsbG8=").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "aGVsbG8=");
    }
}


