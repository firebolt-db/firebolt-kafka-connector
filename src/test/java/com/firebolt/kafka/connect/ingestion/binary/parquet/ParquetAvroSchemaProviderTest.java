package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ParquetAvroSchemaProviderTest {

    @ParameterizedTest
    @CsvSource({
            "boolean-test-table_schemaless,boolean_test_table_schemaless",
            "123-invalid-start,_123_invalid_start",
            "----,record",
            "__,record",
            "my$table$name,my_table_name"
    })
    void recordNameIsSanitizedToValidAvroName(String tableName, String expectedRecordName) {
        TableSchema tableSchema = new TableSchema(tableName);
        tableSchema.addColumn("id", "int", java.sql.Types.INTEGER, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema schema = provider.get(tableSchema);

        assertNotNull(schema);
        assertEquals(expectedRecordName, schema.getName());
        assertEquals("com.firebolt.kafka.connect." + expectedRecordName, schema.getFullName());
    }

    @Test
    void emptyTableNameDefaultsToRecord() {
        TableSchema tableSchema = new TableSchema("");
        tableSchema.addColumn("id", "int", java.sql.Types.INTEGER, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema schema = provider.get(tableSchema);

        assertNotNull(schema);
        assertEquals("record", schema.getName());
        assertEquals("com.firebolt.kafka.connect.record", schema.getFullName());
    }

    @Test
    void buildsRecordWithTableNameAndNamespace() {
        TableSchema schema = new TableSchema("orders");
        schema.addColumn("id", "INTEGER", Types.INTEGER, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);

        assertEquals("orders", avro.getName());
        assertEquals("com.firebolt.kafka.connect", avro.getNamespace());
        assertEquals(1, avro.getFields().size());
        assertEquals("id", avro.getFields().get(0).name());
    }

    @Test
    void nullableFieldIsUnionWithNullFirst() {
        TableSchema schema = new TableSchema("events");
        schema.addColumn("payload", "VARCHAR", Types.VARCHAR, true);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);

        Schema.Field field = avro.getField("payload");
        assertNotNull(field);
        assertEquals(Schema.Type.UNION, field.schema().getType());

        List<Schema> types = field.schema().getTypes();
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertEquals(Schema.Type.STRING, types.get(1).getType());
    }

    @ParameterizedTest
    @CsvSource({
            "bad-name,bad_name",
            "my$table,my_table",
            "1abc,_1abc",
            "naïve,na_ve"
    })
    void columnNameIsSanitizedToValidAvroFieldName(String columnName, String expectedFieldName) {
        TableSchema schema = new TableSchema("t");
        schema.addColumn(columnName, "VARCHAR", Types.VARCHAR, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);

        assertNull(avro.getField(columnName));
        assertNotNull(avro.getField(expectedFieldName));
    }

    @Test
    void nonNullableIntFieldIsInt() {
        TableSchema schema = new TableSchema("numbers");
        schema.addColumn("count", "INTEGER", Types.INTEGER, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);

        Schema.Field field = avro.getField("count");
        assertNotNull(field);
        assertEquals(Schema.Type.INT, field.schema().getType());
    }

    @ParameterizedTest
    @CsvSource({
            // sqlTypeName, expectedAvroBaseType
            "INTEGER,INT",
            "BIGINT,LONG",
            "REAL,FLOAT",
            "DOUBLE,DOUBLE",
            "BOOLEAN,BOOLEAN",
            "BINARY,BYTES",
            "DATE,INT",
            "TIMESTAMP,LONG",
            "TIMESTAMP_WITH_TIMEZONE,LONG",
            "NUMERIC,BYTES",
            "VARCHAR,STRING"
    })
    void mapsNonNullableSqlTypesToExpectedAvroTypes(String sqlTypeName, String expectedAvroTypeName) throws Exception {
        int sqlType = sqlTypeConstant(sqlTypeName);
        Schema.Type expectedBase = Schema.Type.valueOf(expectedAvroTypeName);

        TableSchema schema = new TableSchema("t");
        if ("NUMERIC".equals(sqlTypeName)) {
            schema.addColumn("col", sqlTypeName, sqlType, false, 20, 5);
        } else {
            schema.addColumn("col", sqlTypeName, sqlType, false);
        }

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("col");
        assertNotNull(field);
        assertEquals(expectedBase, field.schema().getType());
        if ("NUMERIC".equals(sqlTypeName)) {
            assertNotNull(field.schema().getLogicalType());
            assertEquals("decimal", field.schema().getLogicalType().getName());
            LogicalTypes.Decimal dec = (LogicalTypes.Decimal) field.schema().getLogicalType();
            assertEquals(20, dec.getPrecision());
            assertEquals(5, dec.getScale());
        }
    }

    @ParameterizedTest
    @CsvSource({
            // sqlTypeName, expectedAvroBaseType (wrapped in union with null)
            "INTEGER,INT",
            "BIGINT,LONG",
            "REAL,FLOAT",
            "DOUBLE,DOUBLE",
            "BOOLEAN,BOOLEAN",
            "BINARY,BYTES",
            "DATE,INT",
            "TIMESTAMP,LONG",
            "TIMESTAMP_WITH_TIMEZONE,LONG",
            "NUMERIC,BYTES",
            "VARCHAR,STRING"
    })
    void mapsNullableSqlTypesToUnionWithNull(String sqlTypeName, String expectedAvroTypeName) throws Exception {
        int sqlType = sqlTypeConstant(sqlTypeName);
        Schema.Type expectedBase = Schema.Type.valueOf(expectedAvroTypeName);

        TableSchema schema = new TableSchema("t");
        if ("NUMERIC".equals(sqlTypeName)) {
            schema.addColumn("col", sqlTypeName, sqlType, true, 18, 4);
        } else {
            schema.addColumn("col", sqlTypeName, sqlType, true);
        }

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("col");
        assertNotNull(field);

        assertEquals(Schema.Type.UNION, field.schema().getType());
        List<Schema> types = field.schema().getTypes();
        assertEquals(2, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertEquals(expectedBase, types.get(1).getType());
        if ("NUMERIC".equals(sqlTypeName)) {
            assertNotNull(types.get(1).getLogicalType());
            assertEquals("decimal", types.get(1).getLogicalType().getName());
            LogicalTypes.Decimal dec = (LogicalTypes.Decimal) types.get(1).getLogicalType();
            assertEquals(18, dec.getPrecision());
            assertEquals(4, dec.getScale());
        }
    }

    private static int sqlTypeConstant(String name) throws Exception {
        Field f = Types.class.getField(name);
        return f.getInt(null);
    }

    @ParameterizedTest
    @CsvSource({
            // sqlTypeName, expectedLogicalTypeName, expectedBaseType
            "DATE,date,INT",
            "TIMESTAMP,timestamp-micros,LONG",
            "TIMESTAMP_WITH_TIMEZONE,timestamp-micros,LONG"
    })
    void setsLogicalTypeForSqlTypes(String sqlTypeName, String expectedLogicalTypeName, String expectedBaseType) throws Exception {
        int sqlType = sqlTypeConstant(sqlTypeName);
        Schema.Type expectedBase = Schema.Type.valueOf(expectedBaseType);

        TableSchema schema = new TableSchema("t");
        schema.addColumn("col", sqlTypeName, sqlType, true);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("col");
        assertNotNull(field);

        assertEquals(Schema.Type.UNION, field.schema().getType());
        List<Schema> types = field.schema().getTypes();
        assertEquals(2, types.size());
        Schema colSchemna = types.get(1);
        assertEquals(expectedBase, colSchemna.getType());
        assertNotNull(colSchemna.getLogicalType());
        assertEquals(expectedLogicalTypeName, colSchemna.getLogicalType().getName());
    }

    @Test
    void nonNullableArrayBigintHasItemUnionNullLong() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr", "array(bigint)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.LONG, itemTypes.get(1).getType());
        assertNull(itemTypes.get(1).getLogicalType());
    }

    @Test
    void nonNullableArrayTimestampHasItemUnionNullTimestampMicrosLong() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_ts", "array(timestamp)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_ts");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.LONG, itemTypes.get(1).getType());
        assertNotNull(itemTypes.get(1).getLogicalType());
        assertEquals("timestamp-micros", itemTypes.get(1).getLogicalType().getName());
    }

    @Test
    void nonNullableArrayDoubleHasItemUnionNullDouble() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_double", "array(double)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_double");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.DOUBLE, itemTypes.get(1).getType());
    }

    @Test
    void nonNullableArrayDateHasItemUnionNullDateInt() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_date", "array(date)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_date");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.INT, itemTypes.get(1).getType());
        assertNotNull(itemTypes.get(1).getLogicalType());
        assertEquals("date", itemTypes.get(1).getLogicalType().getName());
    }

    @Test
    void nonNullableArrayTextHasItemUnionNullString() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_text", "array(text)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_text");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.STRING, itemTypes.get(1).getType());
    }

    @Test
    void nonNullableArrayBooleanHasItemUnionNullBoolean() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_bool", "array(boolean)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_bool");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.BOOLEAN, itemTypes.get(1).getType());
    }

    @Test
    void nonNullableArrayByteaHasItemUnionNullBytes() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_bytea", "array(bytea)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_bytea");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.BYTES, itemTypes.get(1).getType());
    }

    @Test
    void nonNullableByteaHasBytes() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("b", "bytea", Types.BINARY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("b");
        assertNotNull(field);
        assertEquals(Schema.Type.BYTES, field.schema().getType());
    }

    @Test
    void nonNullableArrayDecimalHasItemUnionNullDecimalBytesWithPrecisionScale() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_dec", "array(numeric)", Types.ARRAY, false, 22, 6);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_dec");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.BYTES, itemTypes.get(1).getType());
        assertNotNull(itemTypes.get(1).getLogicalType());
        assertEquals("decimal", itemTypes.get(1).getLogicalType().getName());
        LogicalTypes.Decimal dec = (LogicalTypes.Decimal) itemTypes.get(1).getLogicalType();
        assertEquals(22, dec.getPrecision());
        assertEquals(6, dec.getScale());
    }

    @Test
    void nonNullableArrayTimestamptzHasItemUnionNullTimestampMicrosLong() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_tstz", "array(timestamptz)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_tstz");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());

        assertEquals(Schema.Type.LONG, itemTypes.get(1).getType());
        assertNotNull(itemTypes.get(1).getLogicalType());
        assertEquals("timestamp-micros", itemTypes.get(1).getLogicalType().getName());
    }

    @Test
    void nonNullableArrayRealHasItemUnionNullFloat() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_real", "array(real)", Types.ARRAY, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("arr_real");
        assertNotNull(field);

        assertEquals(Schema.Type.ARRAY, field.schema().getType());
        Schema element = field.schema().getElementType();
        assertEquals(Schema.Type.UNION, element.getType());
        List<Schema> itemTypes = element.getTypes();
        assertEquals(Schema.Type.NULL, itemTypes.get(0).getType());
        assertEquals(Schema.Type.FLOAT, itemTypes.get(1).getType());
    }
}


