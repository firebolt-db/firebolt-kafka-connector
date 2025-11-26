package com.firebolt.kafka.connect.ingestion.binary;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.preparedstatement.FireboltParquetStatement;
import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.IngestionService;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.ingestion.binary.parquet.ParquetDataGenerator;
import com.firebolt.kafka.connect.reporter.ErrorReporter;
import com.google.common.annotations.VisibleForTesting;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinaryIngestionService implements IngestionService {

    private static final String INSERT_SQL_TEMPLATE = "INSERT INTO \"%s\" SELECT * FROM read_parquet('upload://%s')";

    // there are no restrictions on the name of the multi part representing the parquet file. The only restriction is that it should match
    // this regex: [-A-Za-z0-9._~:\/?#\[\]@!$&'()*+,;=]+ and must be unique if multiple parts are send in the same request
    private static final String MULTIPART_BINARY_FILENAME = "binary";

    private BinaryDataGenerator binaryDataGenerator;
    private TableSchema tableSchema;
    private Connection connection;

    public BinaryIngestionService(Connection connection, ErrorReporter errorReporter, boolean errorTolerance, TableSchema tableSchema) {
        this(new ParquetDataGenerator(errorReporter, errorTolerance), tableSchema, connection);
    }

    @VisibleForTesting
    BinaryIngestionService(BinaryDataGenerator binaryDataGenerator, TableSchema tableSchema, Connection connection) {
        this.binaryDataGenerator = binaryDataGenerator;
        this.tableSchema = tableSchema;
        this.connection = connection;
    }

    @Override
    public void addRecords(List<AbstractFireboltRecord> fireboltRecords) throws SQLException {
        if (fireboltRecords == null || fireboltRecords.isEmpty()) {
            log.info("No records to ingest.");
            return;
        }

        // Generate Parquet content
        OutputStream out = binaryDataGenerator.generate(fireboltRecords, tableSchema);

        byte[] parquetBytes;
        if (out instanceof java.io.ByteArrayOutputStream) {
            parquetBytes = ((java.io.ByteArrayOutputStream) out).toByteArray();
        } else {
            throw new SQLException("Unexpected output stream type from binaryDataGenerator; expected in-memory stream.");
        }

        String sql = generateInsertSqlStatement();
        log.debug("Created the sql statement: {}. Parquet file has: {} bytes", sql, parquetBytes.length);

        try {
            FireboltConnection fireboltConnection = connection.unwrap(FireboltConnection.class);
            FireboltParquetStatement parquetStatement = fireboltConnection.createParquetStatement();
            Map<String, byte[]> parquetFiles = new HashMap<>();
            parquetFiles.put(MULTIPART_BINARY_FILENAME, parquetBytes);
            parquetStatement.execute(sql, parquetFiles);
            parquetStatement.close();
        } catch (Exception e) {
            throw new SQLException("Failed to upload parquet content", e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Failed to gracefully close the ingestion service");
        }
    }

    private String generateInsertSqlStatement() {
        return String.format(INSERT_SQL_TEMPLATE, tableSchema.getTableName(), MULTIPART_BINARY_FILENAME);
    }

}
