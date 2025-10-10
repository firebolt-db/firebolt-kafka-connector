package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class SchemalessTimestampDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessTimestampDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessTimestampDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "timestamp", java.sql.Types.TIMESTAMP, true);
    }

    @Test
    void acceptsIsoLocalDateTimeString() throws SQLException {
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value("2024-01-15T14:30:45").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "2024-01-15T14:30:45");
    }

    @Test
    void convertsEpochMillisToTimestamp() throws SQLException {
        long millis = 1705336245000L;
        KafkaMessageColumnValue value = KafkaMessageColumnValue.builder().value(millis).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        ArgumentCaptor<Timestamp> captor = ArgumentCaptor.forClass(Timestamp.class);
        verify(mockStatement).setTimestamp(org.mockito.ArgumentMatchers.eq(1), captor.capture());
        assertEquals(new Timestamp(millis), captor.getValue());
    }
}


