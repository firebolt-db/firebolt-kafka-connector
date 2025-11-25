package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ParquetAvroSchemaProviderTest {

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
        schema.addColumn("col", sqlTypeName, sqlType, false);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("col");
        assertNotNull(field);
        assertEquals(expectedBase, field.schema().getType());
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
        schema.addColumn("col", sqlTypeName, sqlType, true);

        ParquetAvroSchemaProvider provider = new ParquetAvroSchemaProvider();
        Schema avro = provider.get(schema);
        Schema.Field field = avro.getField("col");
        assertNotNull(field);

        assertEquals(Schema.Type.UNION, field.schema().getType());
        List<Schema> types = field.schema().getTypes();
        assertEquals(2, types.size());
        assertEquals(Schema.Type.NULL, types.get(0).getType());
        assertEquals(expectedBase, types.get(1).getType());
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
    void nonNullableArrayDecimalHasItemUnionNullString() {
        TableSchema schema = new TableSchema("t");
        schema.addColumn("arr_dec", "array(numeric)", Types.ARRAY, false);

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


