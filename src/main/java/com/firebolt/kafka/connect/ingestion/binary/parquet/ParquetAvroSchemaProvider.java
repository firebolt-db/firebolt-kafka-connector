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
            Schema fieldSchema = mapSqlTypeToAvro(column);
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
     * @return
     */

    private Schema mapSqlTypeToAvro(TableSchema.Column tableColumn) {
        boolean isNullable = tableColumn.isNullable();
        Schema base;
        switch (tableColumn.getSqlType()) {
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
            case Types.ARRAY:
                base = createArraySchema(tableColumn);
                break;
            case Types.NUMERIC:
            case Types.VARCHAR:
            default:
                base = Schema.create(Schema.Type.STRING);
                break;
        }
        if (isNullable) {
            return Schema.createUnion(Schema.create(Schema.Type.NULL), base);
        }
        return base;
    }

    /**
     * Builds an Avro array schema. For now, supports arrays of integers.
     * - Elements are nullable: union(null, int)
     * - Field nullability (array itself nullable) is handled by the caller.
     */
    private Schema createArraySchema(TableSchema.Column tableColumn) {
        String dataType = tableColumn.getDataType() == null ? "" : tableColumn.getDataType().toLowerCase();

        boolean isIntArray = dataType.startsWith("array(int");
        boolean isBigIntArray = dataType.startsWith("array(bigint");
        boolean isTimestampArray = dataType.startsWith("array(timestamp");
        boolean isTimestamptzArray = dataType.startsWith("array(timestamptz");
        boolean isRealArray = dataType.startsWith("array(real");

        // NOTE will add more datatypes as we develop

        Schema itemSchema;
        if (isIntArray) {
            itemSchema = SchemaBuilder.unionOf().nullType().and().intType().endUnion();
        } else if (isBigIntArray) {
            itemSchema = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
        } else if (isTimestampArray || isTimestamptzArray) {
            Schema tsMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            itemSchema = SchemaBuilder.unionOf().nullType().and().type(tsMicros).endUnion();
        } else if (isRealArray) {
            itemSchema = SchemaBuilder.unionOf().nullType().and().floatType().endUnion();
        } else {
            itemSchema = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
        }

        return SchemaBuilder.array().items(itemSchema);
    }
}
