package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import java.sql.Types;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class ParquetAvroSchemaProvider {

    private static final String SCHEMA_NAMESPACE = "com.firebolt.kafka.connect";

    /**
     * Generates the avro parquet schema based on the table schema
     *
     * @param tableSchema - the Firebolt table schema
     * @return
     */
    public Schema get(TableSchema tableSchema) {
        // Use a simple, valid Avro record name; field names come from table columns.
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(tableSchema.getTableName())
                .namespace(SCHEMA_NAMESPACE)
                .fields();

        for (TableSchema.Column column : tableSchema.getColumns()) {
            String name = column.getName();
            boolean nullable = column.isNullable();
            Schema fieldSchema = mapSqlTypeToAvro(column.getSqlType(), nullable);
            fields.name(name).type(fieldSchema).noDefault();
        }
        return fields.endRecord();
    }

    /**
     * This is a mapping of the Firebolt Data Types to Sql types
     * integer -> Types.INTEGER
     * bigint -> Types.BIGINT
     * numeric -> Types.NUMERIC
     * real -> Types.REAL
     * double -> Types.DOUBLE
     *
     * boolean -> Types.BOOLEAN
     *
     * date -> Types.DATE
     * timestamp -> Types.TIMESTAMP
     * timestamptz -> Types.TIMESTAMP_WITH_TIMEZONE
     *
     * text -> Types.VARCHAR
     * bytea -> Types.BINARY
     *
     * array -> Types.ARRAY
     * geography -> Types.VARCHAR
     * struct -> Types.VARCHAR
     *
     *
     * @param sqlType
     * @param nullable
     * @return
     */

    private static Schema mapSqlTypeToAvro(int sqlType, boolean nullable) {
        Schema base;
        switch (sqlType) {
            case Types.INTEGER:
                base = Schema.create(Schema.Type.INT);
                break;
            case Types.BIGINT:
                base = Schema.create(Schema.Type.LONG);
                break;
            case Types.REAL:
                base = Schema.create(Schema.Type.FLOAT);
                break;
            case Types.DOUBLE:
                base = Schema.create(Schema.Type.DOUBLE);
                break;
            case Types.BOOLEAN:
                base = Schema.create(Schema.Type.BOOLEAN);
                break;
            case Types.BINARY:
                base = Schema.create(Schema.Type.BYTES);
                break;
            case Types.DATE:
                // dates are represented as the number of days since epoch
                base = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // timestamp and timestamp tz have a microseconds precision
                base = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
                break;
            case Types.NUMERIC:
            case Types.VARCHAR:
            default:
                base = Schema.create(Schema.Type.STRING);
                break;
        }
        if (nullable) {
            return Schema.createUnion(Schema.create(Schema.Type.NULL), base);
        }
        return base;
    }
}
