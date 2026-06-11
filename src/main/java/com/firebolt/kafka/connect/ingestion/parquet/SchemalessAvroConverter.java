package com.firebolt.kafka.connect.ingestion.parquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Converts schemaless records (JSON with {@code schemas.enable=false}, arriving as
 * {@code Map<String, Object>}) into Avro records for Parquet upload.
 *
 * <p>This is a mechanical mapping of the Java runtime types the JsonConverter produces —
 * not content-based inference. Integral numbers become longs, floating point becomes
 * doubles, lists become arrays, and nested objects are serialized to JSON strings.
 * When a field's type conflicts across records in a batch, it is promoted (long+double →
 * double, anything else → string). All casting to the target column types happens
 * server-side in the INSERT ... SELECT.
 *
 * <p>This path is an interim solution until Firebolt's {@code read_json} ships, at which
 * point schemaless records are passed through as raw NDJSON instead.
 */
class SchemalessAvroConverter {

    private static final ObjectMapper JSON = new ObjectMapper();

    interface BadRecordHandler {
        /** Either swallows the bad record (DLQ) or throws. */
        void handle(SinkRecord record, RuntimeException cause);
    }

    private enum Kind { UNKNOWN, BOOLEAN, LONG, DOUBLE, STRING, BYTES, ARRAY }

    private static final class Inferred {
        Kind kind = Kind.UNKNOWN;
        Inferred element; // only for ARRAY
    }

    AvroBatch toAvro(List<SinkRecord> records, BadRecordHandler badRecordHandler) {
        List<SinkRecord> usable = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            if (record.value() instanceof Map) {
                usable.add(record);
            } else {
                badRecordHandler.handle(record, new RecordConversionException(
                        "Schemaless record value is not a JSON object: " + record.value().getClass().getName()));
            }
        }

        // pass 1: union of fields across the batch, with type promotion on conflict
        Map<String, Inferred> fields = new LinkedHashMap<>();
        for (SinkRecord record : usable) {
            ((Map<?, ?>) record.value()).forEach((key, value) ->
                    promote(fields.computeIfAbsent(String.valueOf(key), k -> new Inferred()), value));
        }

        Map<String, String> fieldToSource = sanitizeFieldNames(fields.keySet());
        Schema schema = buildSchema(fields, fieldToSource);

        // pass 2: coerce each record to the inferred schema
        List<GenericRecord> avroRecords = new ArrayList<>(usable.size());
        for (SinkRecord record : usable) {
            try {
                avroRecords.add(toRecord((Map<?, ?>) record.value(), fields, fieldToSource, schema));
            } catch (RuntimeException e) {
                badRecordHandler.handle(record, new RecordConversionException(
                        "Failed to convert schemaless record to Parquet representation", e));
            }
        }

        return new AvroBatch(schema, avroRecords, fieldToSource);
    }

    private void promote(Inferred inferred, Object value) {
        Kind valueKind = kindOf(value);
        if (valueKind == Kind.UNKNOWN) {
            return; // nulls don't constrain the type
        }

        if (valueKind == Kind.ARRAY) {
            if (inferred.kind == Kind.UNKNOWN) {
                inferred.kind = Kind.ARRAY;
                inferred.element = new Inferred();
            } else if (inferred.kind != Kind.ARRAY) {
                inferred.kind = Kind.STRING;
                return;
            }
            ((List<?>) value).forEach(element -> promote(inferred.element, element));
            return;
        }

        if (inferred.kind == Kind.UNKNOWN) {
            inferred.kind = valueKind;
        } else if (inferred.kind != valueKind) {
            boolean numeric = (inferred.kind == Kind.LONG || inferred.kind == Kind.DOUBLE)
                    && (valueKind == Kind.LONG || valueKind == Kind.DOUBLE);
            inferred.kind = numeric ? Kind.DOUBLE : Kind.STRING;
        }
    }

    private Kind kindOf(Object value) {
        if (value == null) {
            return Kind.UNKNOWN;
        }
        if (value instanceof Boolean) {
            return Kind.BOOLEAN;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return Kind.LONG;
        }
        if (value instanceof Float || value instanceof Double) {
            return Kind.DOUBLE;
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return Kind.BYTES;
        }
        if (value instanceof List) {
            return Kind.ARRAY;
        }
        // String, BigDecimal/BigInteger (precision-safe as text), nested Map (JSON), anything else
        return Kind.STRING;
    }

    private Map<String, String> sanitizeFieldNames(Iterable<String> sourceNames) {
        Map<String, String> fieldToSource = new LinkedHashMap<>();
        for (String source : sourceNames) {
            String sanitized = source.replaceAll("[^A-Za-z0-9_]", "_");
            if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
                sanitized = "_" + sanitized;
            }
            String candidate = sanitized;
            for (int i = 2; fieldToSource.containsKey(candidate); i++) {
                candidate = sanitized + "_" + i;
            }
            fieldToSource.put(candidate, source);
        }
        return fieldToSource;
    }

    private Schema buildSchema(Map<String, Inferred> fields, Map<String, String> fieldToSource) {
        SchemaBuilder.FieldAssembler<Schema> assembler =
                SchemaBuilder.record("KafkaRecord").namespace("com.firebolt.kafka.connect").fields();
        for (Map.Entry<String, String> entry : fieldToSource.entrySet()) {
            Schema fieldSchema = avroType(fields.get(entry.getValue()));
            assembler = assembler.name(entry.getKey())
                    .type(Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema))
                    .withDefault(null);
        }
        return assembler.endRecord();
    }

    private Schema avroType(Inferred inferred) {
        switch (inferred.kind) {
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case ARRAY:
                // array elements are nullable to tolerate nulls inside JSON arrays
                return Schema.createArray(
                        Schema.createUnion(Schema.create(Schema.Type.NULL), avroType(inferred.element)));
            default:
                // UNKNOWN (all nulls) and STRING
                return Schema.create(Schema.Type.STRING);
        }
    }

    private GenericRecord toRecord(Map<?, ?> value, Map<String, Inferred> fields,
                                   Map<String, String> fieldToSource, Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Map.Entry<String, String> entry : fieldToSource.entrySet()) {
            Object raw = value.containsKey(entry.getValue()) ? value.get(entry.getValue()) : null;
            record.put(entry.getKey(), coerce(raw, fields.get(entry.getValue())));
        }
        return record;
    }

    private Object coerce(Object value, Inferred inferred) {
        if (value == null) {
            return null;
        }
        switch (inferred.kind) {
            case BOOLEAN:
                return value;
            case LONG:
                return ((Number) value).longValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BYTES:
                return value instanceof ByteBuffer ? value : ByteBuffer.wrap((byte[]) value);
            case ARRAY:
                List<Object> elements = new ArrayList<>(((List<?>) value).size());
                for (Object element : (List<?>) value) {
                    elements.add(coerce(element, inferred.element));
                }
                return elements;
            default:
                return asString(value);
        }
    }

    private String asString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Map || value instanceof List) {
            try {
                return JSON.writeValueAsString(value);
            } catch (Exception e) {
                throw new RecordConversionException("Failed to serialize nested value to JSON", e);
            }
        }
        return String.valueOf(value);
    }
}
