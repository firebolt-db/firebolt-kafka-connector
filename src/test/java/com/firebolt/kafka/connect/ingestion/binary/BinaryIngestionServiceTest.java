package com.firebolt.kafka.connect.ingestion.binary;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.TableSchema;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

    @Test
    void addRecordsReturnsEarlyWhenNull() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema);

        service.addRecords(null);

        verifyNoInteractions(mockDataGenerator);
    }

    @Test
    void addRecordsReturnsEarlyWhenEmpty() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema);

        service.addRecords(Collections.emptyList());

        verifyNoInteractions(mockDataGenerator);
    }

    @Test
    void addRecordsUploadsParquetBytesWithCorrectSqlAndPart() throws Exception {
        TableSchema tableSchema = new TableSchema("orders");
        BinaryIngestionService service = new BinaryIngestionService(mockDataGenerator, tableSchema);

        AbstractFireboltRecord record = mock(AbstractFireboltRecord.class);
        List<AbstractFireboltRecord> records = List.of(record);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("parquet".getBytes());
        when(mockDataGenerator.generate(eq(records), eq(tableSchema))).thenReturn(baos);

        service.addRecords(records);

        verify(mockDataGenerator, times(1)).generate(eq(records), eq(tableSchema));
    }

}


