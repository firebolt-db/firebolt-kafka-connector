package com.firebolt.kafka.connect.datatype.converter.schemaless;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class SchemalessDateDataTypeConverterTest {

    @Mock
    private PreparedStatement mockStatement;

    private SchemalessDateDataTypeConverter converter;
    private TableSchema.Column testColumn;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new SchemalessDateDataTypeConverter();
        testColumn = new TableSchema.Column("test_column", "date", java.sql.Types.DATE, true);
    }

    @Test
    void convertsEpochDaysToDate() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value(1).build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        ArgumentCaptor<Date> captor = ArgumentCaptor.forClass(Date.class);
        verify(mockStatement).setDate(org.mockito.ArgumentMatchers.eq(1), captor.capture());
        assertEquals(Date.valueOf(java.time.LocalDate.ofEpochDay(1)), captor.getValue());
    }

    @Test
    void passesIsoStringThrough() throws SQLException {
        SchemaKafkaMessageColumnValue value = SchemaKafkaMessageColumnValue.builder().value("2024-01-15").build();
        converter.convertAndSet(mockStatement, 1, value, testColumn);
        verify(mockStatement).setString(1, "2024-01-15");
    }
}


