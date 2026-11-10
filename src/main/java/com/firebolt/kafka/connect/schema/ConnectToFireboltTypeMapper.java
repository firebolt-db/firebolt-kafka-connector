package com.firebolt.kafka.connect.schema;

import org.apache.kafka.connect.data.Schema;

/**
 * Maps Kafka Connect field schemas to Firebolt SQL type strings for use in DDL statements
 * (e.g. ALTER TABLE ADD COLUMN).
 *
 * <p>Logical types (Decimal, Date, Timestamp) take precedence over the raw primitive type.
 * ARRAY types are supported recursively; element nullability is derived from
 * {@link Schema#isOptional()} on the element schema.
 *
 * <p>Returns {@code null} for types that have no Firebolt equivalent (STRUCT, MAP) — callers
 * should skip such fields and emit a warning.
 *
 * <p>The returned string contains <em>only the type</em>, e.g. {@code TEXT},
 * {@code BIGINT}, {@code ARRAY(INTEGER NULL)}, {@code NUMERIC(38, 5)}.
 * Callers are responsible for appending the column-level nullability clause
 * ({@code NULL} or {@code NOT NULL}).
 */
public class ConnectToFireboltTypeMapper {

    private ConnectToFireboltTypeMapper() {}

    public static String toFireboltType(Schema schema) {
        // Logical types take precedence over the raw primitive type.
        String logicalName = schema.name();
        if (org.apache.kafka.connect.data.Decimal.LOGICAL_NAME.equals(logicalName)) {
            int scale = Integer.parseInt(schema.parameters().get(org.apache.kafka.connect.data.Decimal.SCALE_FIELD));
            return "NUMERIC(38, " + scale + ")";
        }
        if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logicalName)) {
            return "DATE";
        }
        if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(logicalName)) {
            return "TIMESTAMP";
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BYTEA";
            case ARRAY: {
                Schema elementSchema = schema.valueSchema();
                String elementType = toFireboltType(elementSchema);
                if (elementType == null) {
                    return null;
                }
                // Firebolt distinguishes ARRAY(T NULL) from ARRAY(T NOT NULL).
                String elementNullability = elementSchema.isOptional() ? " NULL" : " NOT NULL";
                return "ARRAY(" + elementType + elementNullability + ")";
            }
            default:
                // STRUCT, MAP — no single Firebolt type equivalent; caller should skip.
                return null;
        }
    }
}
