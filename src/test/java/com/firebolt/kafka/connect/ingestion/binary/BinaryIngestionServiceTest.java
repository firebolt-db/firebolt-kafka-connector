package com.firebolt.kafka.connect.ingestion.binary;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.TableSchema;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinaryIngestionServiceTest {

    @Mock
    private BinaryDataGenerator mockDataGenerator;

    @Mock
    private FireboltConnection mockFireboltConnection;

    @Mock
    private FireboltParquetStatement mockParquetStatement;

    @Test
    void addRecordsReturnsEarlyWhenNull() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema, mockFireboltConnection);

        service.addRecords(null);

        verifyNoInteractions(mockDataGenerator);
    }

    @Test
    void addRecordsReturnsEarlyWhenEmpty() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema, mockFireboltConnection);

        service.addRecords(Collections.emptyList());

        verifyNoInteractions(mockDataGenerator);
    }

    @Test
    void addRecordsUploadsParquetBytesWithCorrectSqlAndPart() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        when(mockFireboltConnection.unwrap(FireboltConnection.class)).thenReturn(mockFireboltConnection);
        when(mockFireboltConnection.createParquetStatement()).thenReturn(mockParquetStatement);
        
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema, mockFireboltConnection);

        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        List<AbstractFireboltRecord> records = List.of(record);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("parquet".getBytes());
        when(mockDataGenerator.generate(eq(records), eq(tableSchema))).thenReturn(baos);

        service.addRecords(records);

        verify(mockDataGenerator, times(1)).generate(eq(records), eq(tableSchema));
        verify(mockFireboltConnection, times(1)).createParquetStatement();
        verify(mockParquetStatement, times(1)).execute(any(String.class), any(Map.class));
        verify(mockParquetStatement, times(1)).close();
    }

}


