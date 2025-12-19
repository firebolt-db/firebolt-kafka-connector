package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.TimestampUtil;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessTimestamptzDataTypeConverter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class SchemalessTimestamptzDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessTimestamptzDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessTimestamptzDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "timestamptz", java.sql.Types.TIMESTAMP_WITH_TIMEZONE, true);
    }

    @Test
    void acceptsValidString() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("2024-01-15T14:30:45.123456Z").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "2024-01-15T14:30:45.123456Z");
    }

    @Test
    void convertsEpochMillisToOffsetDateTime() throws SQLException {
        long millis = 1705336245123L;
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(millis).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mockStatement).setObject(org.mockito.ArgumentMatchers.eq(1), captor.capture());
        OffsetDateTime expected = TimestampUtil.asOffsetDateTime(millis);
        assertEquals(expected, (OffsetDateTime) captor.getValue());
    }
}


