package com.firebolt.kafka.connect.integration.protobuf;

import com.firebolt.kafka.connect.utils.TestTag;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Protobuf serialization via Confluent's ProtobufConverter.
 *
 * <p>Confluent's ProtobufConverter deserializes Protobuf messages into Kafka Connect Struct objects,
 * which are then handled by the existing SchemaBasedRecordConverter — no new converter code needed.
 *
 * <p>Records use DynamicMessage to avoid compiled .proto files, mirroring the Avro GenericRecord approach.
 */
@Slf4j
@Tag(TestTag.SERIALIZATION)
public class ProtobufAllDataTypesSerializerTest extends ProtobufBaseIntegrationTest {

    private static final String TABLE_NAME = "all_data_types_test_table_protobuf";
    private static final String TOPIC_NAME = "all-data-types-test-topic-protobuf";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("all-data-types-protobuf-serializer");
    }

    @AfterEach
    protected void tearDown() {
        cleanupProtobufTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("ingestionTypes")
    void testAllDataTypesProtobufSerializationAndKafkaConnectProcessing(
            Map<String, String> connectorOverride, String testDescription) throws Exception {
        log.info("Running {} for all data types (Protobuf)", testDescription);

        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                tableSchema(), protobufSchema(), connectorOverride);

        // Parse the schema to get the Descriptor for DynamicMessage construction
        ProtobufSchema parsedSchema = new ProtobufSchema(protobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("AllDataTypesRecord");

        List<DynamicMessage> testRecords = generateTestRecords(descriptor);

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (int i = 0; i < testRecords.size(); i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(i + 1), testRecords.get(i))).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, testRecords.size());
        verifyRecordsInFirebolt(testRecords, descriptor);
    }

    @Test
    void testOptionalProtobufFieldsCanBeAbsentForNullableColumns() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                optionalFieldsTableSchema(), optionalFieldsProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(optionalFieldsProtobufSchema().get());
        Descriptor descriptor = parsedSchema.toDescriptor().getFile().findMessageTypeByName("OptionalFieldsRecord");
        List<DynamicMessage> records = List.of(
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 1)
                        .setField(descriptor.findFieldByName("optionalText"), "present")
                        .setField(descriptor.findFieldByName("optionalNumeric"), "123.456789")
                        .build(),
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 2)
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, records.size());

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"optionalText\", \"optionalNumeric\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("present", rs.getString("optionalText"));
            assertEquals(0, new BigDecimal("123.456789").compareTo(rs.getBigDecimal("optionalNumeric")));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertNull(rs.getString("optionalText"));
            assertNull(rs.getBigDecimal("optionalNumeric"));
        }
    }

    /**
     * Exercises nested array support across multiple inner element types — Protobuf models nested
     * arrays as `repeated WrapperMessage { repeated X values; }`, which Confluent's
     * ProtobufConverter surfaces as `List<Struct>` to the connector. Each inner type must round-trip
     * correctly. Inner types that need element-level conversion (timestamp/date/decimal/bytea) are
     * not supported today and are covered separately by `testNestedArrayWithUnsupportedInnerType`.
     */
    @Test
    void testNestedArrayProtobufSerializationWithSqlIngestion() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedArrayTableSchema(), nestedArrayProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedArrayProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("NestedArrayRecord");
        Descriptor intArrayDescriptor = fileDescriptor.findMessageTypeByName("IntArray");
        Descriptor longArrayDescriptor = fileDescriptor.findMessageTypeByName("LongArray");
        Descriptor doubleArrayDescriptor = fileDescriptor.findMessageTypeByName("DoubleArray");
        Descriptor stringArrayDescriptor = fileDescriptor.findMessageTypeByName("StringArray");
        Descriptor boolArrayDescriptor = fileDescriptor.findMessageTypeByName("BoolArray");
        List<DynamicMessage> records = List.of(
                // Two-row record with populated inner arrays for every type.
                DynamicMessage.newBuilder(recordDescriptor)
                        .setField(recordDescriptor.findFieldByName("id"), 1)
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                                intArray(intArrayDescriptor, 1, 2))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                                intArray(intArrayDescriptor, 3, 4))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedLongs"),
                                longArray(longArrayDescriptor, 100L, 200L))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedLongs"),
                                longArray(longArrayDescriptor, 300L))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedDoubles"),
                                doubleArray(doubleArrayDescriptor, 1.5, 2.5))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedDoubles"),
                                doubleArray(doubleArrayDescriptor, 3.5))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedStrings"),
                                stringArray(stringArrayDescriptor, "alpha", "beta"))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedStrings"),
                                stringArray(stringArrayDescriptor, "gamma"))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedBooleans"),
                                boolArray(boolArrayDescriptor, true, false))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedBooleans"),
                                boolArray(boolArrayDescriptor, true))
                        .build(),
                // Asymmetric and ragged: differing inner array sizes per type.
                DynamicMessage.newBuilder(recordDescriptor)
                        .setField(recordDescriptor.findFieldByName("id"), 2)
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                                intArray(intArrayDescriptor, 5))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                                intArray(intArrayDescriptor, 6, 7, 8))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedLongs"),
                                longArray(longArrayDescriptor, Long.MAX_VALUE))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedDoubles"),
                                doubleArray(doubleArrayDescriptor, -1.25, 0.0, 1.25))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedStrings"),
                                stringArray(stringArrayDescriptor))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedStrings"),
                                stringArray(stringArrayDescriptor, "single"))
                        .addRepeatedField(recordDescriptor.findFieldByName("nestedBooleans"),
                                boolArray(boolArrayDescriptor, false))
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(recordDescriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, records.size());

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"nestedInts\", \"nestedLongs\", \"nestedDoubles\", \"nestedStrings\", \"nestedBooleans\" " +
                        "FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)),
                    parseNestedIntegerArray(rs.getString("nestedInts")));
            assertEquals(List.of(List.of(100L, 200L), List.of(300L)),
                    parseNestedLongArray(rs.getString("nestedLongs")));
            assertEquals(List.of(List.of(1.5, 2.5), List.of(3.5)),
                    parseNestedDoubleArray(rs.getString("nestedDoubles")));
            assertEquals(List.of(List.of("alpha", "beta"), List.of("gamma")),
                    parseNestedStringArray(rs.getString("nestedStrings")));
            assertEquals(List.of(List.of(true, false), List.of(true)),
                    parseNestedBooleanArray(rs.getString("nestedBooleans")));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(List.of(List.of(5), List.of(6, 7, 8)),
                    parseNestedIntegerArray(rs.getString("nestedInts")));
            assertEquals(List.of(List.of(Long.MAX_VALUE)),
                    parseNestedLongArray(rs.getString("nestedLongs")));
            assertEquals(List.of(List.of(-1.25, 0.0, 1.25)),
                    parseNestedDoubleArray(rs.getString("nestedDoubles")));
            // First inner string array is empty; second has a single element.
            assertEquals(List.of(List.of(), List.of("single")),
                    parseNestedStringArray(rs.getString("nestedStrings")));
            assertEquals(List.of(List.of(false)),
                    parseNestedBooleanArray(rs.getString("nestedBooleans")));
        }
    }

    /**
     * Triple-nested arrays still round-trip through the connector. Protobuf has no native support
     * for nested repeated fields so the schema layers wrapper messages at every level
     * ({@code repeated WrapperA { repeated WrapperB { repeated int32 values; } values; }}); the
     * connector unwraps each Struct level recursively before handing the array to the JDBC driver.
     */
    @Test
    void testTripleNestedArrayProtobufSerializationWithSqlIngestion() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                tripleNestedArrayTableSchema(), tripleNestedArrayProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(tripleNestedArrayProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("TripleNestedRecord");
        Descriptor intArray = fileDescriptor.findMessageTypeByName("IntArray");
        Descriptor intArrayOfArray = fileDescriptor.findMessageTypeByName("IntArrayOfArray");

        DynamicMessage row1 = DynamicMessage.newBuilder(recordDescriptor)
                .setField(recordDescriptor.findFieldByName("id"), 1)
                .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                        DynamicMessage.newBuilder(intArrayOfArray)
                                .addRepeatedField(intArrayOfArray.findFieldByName("values"), intArray(intArray, 1, 2))
                                .addRepeatedField(intArrayOfArray.findFieldByName("values"), intArray(intArray, 3))
                                .build())
                .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                        DynamicMessage.newBuilder(intArrayOfArray)
                                .addRepeatedField(intArrayOfArray.findFieldByName("values"), intArray(intArray, 4, 5, 6))
                                .build())
                .build();

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "1", row1)).get();
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, 1);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"nestedInts\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(
                    List.of(
                            List.of(List.of(1, 2), List.of(3)),
                            List.of(List.of(4, 5, 6))),
                    parseTripleNestedIntegerArray(rs.getString("nestedInts")));
        }
    }

    /**
     * Protobuf {@code oneof} fields are flattened by the value converter (see
     * {@link ProtobufBaseIntegrationTest#registerProtobufConnector}) into one Connect field per
     * member. Each member maps to its own Firebolt column; only the member set on the wire
     * receives a non-null value, the rest are SQL NULL. Mirrors ClickHouse's flattening
     * behaviour:
     * <a href="https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink#protobuf-schema-support">ClickHouse Kafka Connect Sink Protobuf docs</a>.
     */
    @Test
    void testOneofProtobufFieldFlattensToColumnsWithSqlIngestion() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                oneofTableSchema(), oneofProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(oneofProtobufSchema().get());
        Descriptor descriptor = parsedSchema.toDescriptor().getFile().findMessageTypeByName("OneofRecord");

        // Three records each picking a different oneof branch.
        List<DynamicMessage> records = List.of(
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 1)
                        .setField(descriptor.findFieldByName("textValue"), "hello")
                        .build(),
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 2)
                        .setField(descriptor.findFieldByName("intValue"), 42)
                        .build(),
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 3)
                        .setField(descriptor.findFieldByName("doubleValue"), 3.14)
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, records.size());

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"textValue\", \"intValue\", \"doubleValue\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("hello", rs.getString("textValue"));
            assertNull(rs.getObject("intValue"), "intValue must be SQL NULL when textValue is set");
            assertNull(rs.getObject("doubleValue"), "doubleValue must be SQL NULL when textValue is set");

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertNull(rs.getString("textValue"), "textValue must be SQL NULL when intValue is set");
            assertEquals(42, rs.getInt("intValue"));
            assertNull(rs.getObject("doubleValue"), "doubleValue must be SQL NULL when intValue is set");

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertNull(rs.getString("textValue"), "textValue must be SQL NULL when doubleValue is set");
            assertNull(rs.getObject("intValue"), "intValue must be SQL NULL when doubleValue is set");
            assertEquals(3.14, rs.getDouble("doubleValue"), 1e-9);
        }
    }

    /**
     * A {@code oneof} branch may itself be a {@code repeated} field via wrapper messages.
     * After flattening, each branch is a top-level Connect field whose schema reflects the
     * underlying repeated-message shape; the connector's array converter picks it up the same way
     * as a regular {@code array(<scalar>)} (with the wrapper-Struct unwrap path used for nested
     * arrays).
     */
    @Test
    void testOneofOfArrayProtobufFieldFlattensToArrayColumnWithSqlIngestion() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                oneofOfArrayTableSchema(), oneofOfArrayProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(oneofOfArrayProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("OneofOfArrayRecord");
        Descriptor stringArray = fileDescriptor.findMessageTypeByName("StringArray");
        Descriptor intArray = fileDescriptor.findMessageTypeByName("IntArray");

        List<DynamicMessage> records = List.of(
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 1)
                        .setField(descriptor.findFieldByName("textArray"),
                                stringArray(stringArray, "alpha", "beta"))
                        .build(),
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 2)
                        .setField(descriptor.findFieldByName("intArray"),
                                intArray(intArray, 7, 8, 9))
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, records.size());

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"textArray\", \"intArray\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(List.of("alpha", "beta"), parsePostgreSQLArray(rs.getString("textArray")));
            assertNullOrEmptyArray("intArray", rs.getString("intArray"), 0);

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertNullOrEmptyArray("textArray", rs.getString("textArray"), 1);
            assertEquals(List.of(7, 8, 9),
                    parsePostgreSQLArray(rs.getString("intArray")).stream()
                            .map(Integer::parseInt)
                            .collect(Collectors.toList()));
        }
    }

    /**
     * Nested {@code oneof}s — a branch of one {@code oneof} is itself a message containing another
     * {@code oneof} — should fully flatten through both levels with {@code flatten.unions=true}.
     */
    @Test
    void testNestedOneofProtobufFieldFlattensToColumnsWithSqlIngestion() throws Exception {
        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedOneofTableSchema(), nestedOneofProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedOneofProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("NestedOneofRecord");
        Descriptor inner = fileDescriptor.findMessageTypeByName("InnerOneof");

        DynamicMessage rowInnerText = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .setField(descriptor.findFieldByName("nested"),
                        DynamicMessage.newBuilder(inner)
                                .setField(inner.findFieldByName("nestedText"), "deep")
                                .build())
                .build();
        DynamicMessage rowInnerInt = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 2)
                .setField(descriptor.findFieldByName("nested"),
                        DynamicMessage.newBuilder(inner)
                                .setField(inner.findFieldByName("nestedInt"), 99)
                                .build())
                .build();
        DynamicMessage rowFlatBool = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 3)
                .setField(descriptor.findFieldByName("flatBool"), true)
                .build();

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : List.of(rowInnerText, rowInnerInt, rowFlatBool)) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        waitForDataInFirebolt(TABLE_NAME, 3);

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"nestedText\", \"nestedInt\", \"flatBool\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("deep", rs.getString("nestedText"));
            assertNull(rs.getObject("nestedInt"));
            assertNull(rs.getObject("flatBool"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertNull(rs.getString("nestedText"));
            assertEquals(99, rs.getInt("nestedInt"));
            assertNull(rs.getObject("flatBool"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertNull(rs.getString("nestedText"));
            assertNull(rs.getObject("nestedInt"));
            assertEquals(Boolean.TRUE, rs.getObject("flatBool", Boolean.class));
        }
    }

    /**
     * Plain Protobuf nested messages (a non-{@code oneof} sub-message) currently surface as a
     * Connect Struct value targeting a Firebolt {@code STRUCT} column. The connector's
     * {@code SchemaColumnTypeConverterFactory} does not yet provide a STRUCT converter
     * (see {@code FireboltColumnDataType.STRUCT}), so the connector must reject the record and
     * route it to the DLQ rather than silently dropping or corrupting it.
     *
     * <p>TODO: implement a SchemaStructDataTypeConverter so nested protobuf messages and
     * non-flattened Connect Structs ingest into Firebolt STRUCT columns directly. Once Firebolt
     * ships its VARIANT type, the converter should also accept VARIANT columns.
     */
    @Test
    @Tag(TestTag.CONNECTOR)
    void testNestedProtobufMessageTargetingFireboltStructColumnFailsTodayAndRoutesToDlq() throws Exception {
        String dlqTopicName = "dlq-protobuf-nested-message-" + UUID.randomUUID();
        createKafkaTopic(dlqTopicName);

        Map<String, String> connectorOverride = Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.context.headers.enable", "true",
                "ingestion.type", "sql"
        );

        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedMessageStructTableSchema(), nestedMessageProtobufSchema(), connectorOverride);

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedMessageProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("NestedMessageRecord");
        Descriptor inner = fileDescriptor.findMessageTypeByName("Inner");

        DynamicMessage record = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .setField(descriptor.findFieldByName("payload"),
                        DynamicMessage.newBuilder(inner)
                                .setField(inner.findFieldByName("a"), "x")
                                .setField(inner.findFieldByName("b"), 7)
                                .build())
                .build();

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer();
             KafkaConsumer<String, byte[]> dlqConsumer = createDlqConsumer(dlqTopicName)) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "1", record)).get();
            producer.flush();

            int dlqMessages = 0;
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (dlqMessages < 1 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> polled = dlqConsumer.poll(Duration.ofSeconds(5));
                dlqMessages += polled.count();
            }

            assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME),
                    "Nested message records must not land in Firebolt while STRUCT support is missing");
            assertTrue(dlqMessages >= 1,
                    "Expected the nested-message record to land in the DLQ -- if this assertion " +
                            "starts failing, STRUCT support has likely landed and this test should be " +
                            "updated to assert successful ingestion instead.");
        } finally {
            safelyDeleteKafkaTopic(dlqTopicName);
        }
    }

    /**
     * Records that omit a Protobuf field mapped to a NOT NULL Firebolt column should not silently
     * succeed: the connector forwards the absent field as SQL NULL to the database, which rejects
     * the row at insert time (NOT NULL constraint violation). With errors.tolerance="all" + a DLQ
     * topic the failed records must end up in the DLQ rather than the target table.
     */
    @Test
    @Tag(TestTag.CONNECTOR)
    void testAbsentOptionalProtobufFieldFailsForNotNullFireboltColumnAndFlowsToDlq() throws Exception {
        String dlqTopicName = "dlq-protobuf-not-null-" + UUID.randomUUID();
        createKafkaTopic(dlqTopicName);

        Map<String, String> connectorOverride = Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.context.headers.enable", "true",
                "ingestion.type", "sql"
        );

        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                notNullTableSchema(), optionalFieldsProtobufSchema(), connectorOverride);

        ProtobufSchema parsedSchema = new ProtobufSchema(optionalFieldsProtobufSchema().get());
        Descriptor descriptor = parsedSchema.toDescriptor().getFile().findMessageTypeByName("OptionalFieldsRecord");

        // Two records, both missing the optional fields that map to NOT NULL columns. Sending
        // multiple records guarantees we see batch-level rejection regardless of how the connector
        // groups them: the table should still have zero rows after the put attempt(s), and at least
        // one record must arrive in the DLQ.
        List<DynamicMessage> records = List.of(
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 1)
                        .build(),
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 2)
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer();
             KafkaConsumer<String, byte[]> dlqConsumer = createDlqConsumer(dlqTopicName)) {

            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();

            // The connector either fails the batch (errors.tolerance="all" routes everything to
            // DLQ) or fails to even build a complete INSERT (the NOT NULL columns get dropped
            // because all records have them as null) -- in either case nothing must be written.
            // Poll for DLQ messages with a generous timeout so slow CI hosts have a chance to
            // process the records.
            int dlqMessages = 0;
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (dlqMessages < records.size() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> polled = dlqConsumer.poll(Duration.ofSeconds(5));
                dlqMessages += polled.count();
            }

            // The Firebolt table must remain empty: a NOT NULL violation is *not* allowed to be
            // silently swallowed.
            assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME),
                    "No records should land in Firebolt when NOT NULL columns are missing");
            assertTrue(dlqMessages >= 1,
                    "Expected at least one record in DLQ when NOT NULL columns receive null values, got " + dlqMessages);
        } finally {
            safelyDeleteKafkaTopic(dlqTopicName);
        }
    }

    /**
     * Nested arrays whose inner element type needs per-element conversion (e.g. timestamp/date/
     * decimal/bytea) are explicitly rejected by SchemaArrayDataTypeConverter today. The connector
     * therefore reports the failed record to the DLQ rather than corrupting the row.
     */
    @Test
    @Tag(TestTag.CONNECTOR)
    void testNestedArrayWithUnsupportedInnerTypeIsRoutedToDlq() throws Exception {
        String dlqTopicName = "dlq-protobuf-nested-unsupported-" + UUID.randomUUID();
        createKafkaTopic(dlqTopicName);

        Map<String, String> connectorOverride = Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.context.headers.enable", "true",
                "ingestion.type", "sql"
        );

        setupProtobufTestResources(
                TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedTimestampTableSchema(), nestedTimestampProtobufSchema(), connectorOverride);

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedTimestampProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("NestedTimestampRecord");
        Descriptor wrapper = fileDescriptor.findMessageTypeByName("StringArray");
        DynamicMessage record = DynamicMessage.newBuilder(recordDescriptor)
                .setField(recordDescriptor.findFieldByName("id"), 1)
                .addRepeatedField(recordDescriptor.findFieldByName("nestedTimestamps"),
                        stringArray(wrapper, "2024-01-01T00:00:00Z"))
                .build();

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer();
             KafkaConsumer<String, byte[]> dlqConsumer = createDlqConsumer(dlqTopicName)) {

            producer.send(new ProducerRecord<>(TOPIC_NAME, "1", record)).get();
            producer.flush();

            int dlqMessages = 0;
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (dlqMessages < 1 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> polled = dlqConsumer.poll(Duration.ofSeconds(5));
                dlqMessages += polled.count();
            }

            assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME),
                    "No record should land in Firebolt when nested array conversion fails");
            assertTrue(dlqMessages >= 1,
                    "Expected the unsupported nested array record to land in the DLQ");
        } finally {
            safelyDeleteKafkaTopic(dlqTopicName);
        }
    }

    private KafkaConsumer<String, byte[]> createDlqConsumer(String dlqTopicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-dlq-it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(dlqTopicName));
        return consumer;
    }

    // ---------------------------------------------------------------------------
    // Table DDL
    // ---------------------------------------------------------------------------

    private Supplier<String> tableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"colInteger\" INTEGER NOT NULL, " +
                "\"colBigint\" BIGINT, " +
                "\"colNumeric\" NUMERIC(38,9), " +
                "\"colReal\" REAL, " +
                "\"colDoublePrecision\" DOUBLE PRECISION, " +
                "\"colBoolean\" BOOLEAN, " +
                "\"colText\" TEXT, " +
                "\"colDate\" DATE, " +
                "\"colTimestamp\" TIMESTAMP, " +
                "\"colTimestamptz\" TIMESTAMPTZ, " +
                "\"colBytea\" BYTEA, " +
                "\"colArrayText\" ARRAY(TEXT NULL), " +
                "\"colArrayInt\" ARRAY(INTEGER), " +
                "\"colArrayDate\" ARRAY(DATE), " +
                "\"colArrayReal\" ARRAY(REAL), " +
                "\"colArrayNumeric\" ARRAY(NUMERIC), " +
                "\"colArrayDoublePrecision\" ARRAY(DOUBLE PRECISION), " +
                "\"colArrayTimestamptz\" ARRAY(TIMESTAMPTZ), " +
                "\"colArrayTimestamp\" ARRAY(TIMESTAMP) " +
                ");";
    }

    // ---------------------------------------------------------------------------
    // Protobuf schema definition
    //
    // Type mapping rationale:
    //   INT      -> int32  (passed through as Connect INT32)
    //   BIGINT   -> int64  (Connect INT64)
    //   REAL     -> float  (Connect FLOAT32)
    //   DOUBLE   -> double (Connect FLOAT64)
    //   TEXT     -> string (Connect STRING)
    //   DECIMAL  -> string (SchemaDecimalDataTypeConverter accepts STRING)
    //   DATE     -> string (ISO-8601; SchemaDateDataTypeConverter accepts STRING)
    //   TIMESTAMP/TIMESTAMPTZ -> google.protobuf.Timestamp
    //                           ProtobufConverter maps to Connect Timestamp (INT64 millis)
    //   BOOLEAN  -> bool   (Connect BOOLEAN)
    //   BYTEA    -> bytes  (Connect BYTES)
    //   ARRAY    -> repeated fields (Connect ARRAY)
    // ---------------------------------------------------------------------------

    private Supplier<String> protobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "import \"google/protobuf/timestamp.proto\";\n" +
                "message AllDataTypesRecord {\n" +
                "  int32 colInteger = 1;\n" +
                "  int64 colBigint = 2;\n" +
                "  string colNumeric = 3;\n" +
                "  float colReal = 4;\n" +
                "  double colDoublePrecision = 5;\n" +
                "  bool colBoolean = 6;\n" +
                "  string colText = 7;\n" +
                "  string colDate = 8;\n" +
                "  google.protobuf.Timestamp colTimestamp = 9;\n" +
                "  google.protobuf.Timestamp colTimestamptz = 10;\n" +
                "  bytes colBytea = 11;\n" +
                "  repeated string colArrayText = 12;\n" +
                "  repeated int32 colArrayInt = 13;\n" +
                "  repeated string colArrayDate = 14;\n" +
                "  repeated float colArrayReal = 15;\n" +
                "  repeated string colArrayNumeric = 16;\n" +
                "  repeated double colArrayDoublePrecision = 17;\n" +
                "  repeated google.protobuf.Timestamp colArrayTimestamptz = 18;\n" +
                "  repeated google.protobuf.Timestamp colArrayTimestamp = 19;\n" +
                "}\n";
    }

    private Supplier<String> optionalFieldsTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"optionalText\" TEXT, " +
                "\"optionalNumeric\" NUMERIC(38,9) " +
                ");";
    }

    private Supplier<String> optionalFieldsProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message OptionalFieldsRecord {\n" +
                "  int32 id = 1;\n" +
                "  optional string optionalText = 2;\n" +
                "  optional string optionalNumeric = 3;\n" +
                "}\n";
    }

    private Supplier<String> nestedArrayTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"nestedInts\" ARRAY(ARRAY(INTEGER)), " +
                "\"nestedLongs\" ARRAY(ARRAY(BIGINT)), " +
                "\"nestedDoubles\" ARRAY(ARRAY(DOUBLE PRECISION)), " +
                "\"nestedStrings\" ARRAY(ARRAY(TEXT)), " +
                "\"nestedBooleans\" ARRAY(ARRAY(BOOLEAN)) " +
                ");";
    }

    private Supplier<String> nestedArrayProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message IntArray {\n" +
                "  repeated int32 values = 1;\n" +
                "}\n" +
                "message LongArray {\n" +
                "  repeated int64 values = 1;\n" +
                "}\n" +
                "message DoubleArray {\n" +
                "  repeated double values = 1;\n" +
                "}\n" +
                "message StringArray {\n" +
                "  repeated string values = 1;\n" +
                "}\n" +
                "message BoolArray {\n" +
                "  repeated bool values = 1;\n" +
                "}\n" +
                "message NestedArrayRecord {\n" +
                "  int32 id = 1;\n" +
                "  repeated IntArray nestedInts = 2;\n" +
                "  repeated LongArray nestedLongs = 3;\n" +
                "  repeated DoubleArray nestedDoubles = 4;\n" +
                "  repeated StringArray nestedStrings = 5;\n" +
                "  repeated BoolArray nestedBooleans = 6;\n" +
                "}\n";
    }

    private Supplier<String> notNullTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"optionalText\" TEXT NOT NULL, " +
                "\"optionalNumeric\" NUMERIC(38,9) NOT NULL " +
                ");";
    }

    private Supplier<String> nestedTimestampTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"nestedTimestamps\" ARRAY(ARRAY(TIMESTAMP)) " +
                ");";
    }

    private Supplier<String> nestedTimestampProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message StringArray {\n" +
                "  repeated string values = 1;\n" +
                "}\n" +
                "message NestedTimestampRecord {\n" +
                "  int32 id = 1;\n" +
                "  repeated StringArray nestedTimestamps = 2;\n" +
                "}\n";
    }

    private Supplier<String> tripleNestedArrayTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"nestedInts\" ARRAY(ARRAY(ARRAY(INTEGER))) " +
                ");";
    }

    private Supplier<String> tripleNestedArrayProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message IntArray {\n" +
                "  repeated int32 values = 1;\n" +
                "}\n" +
                "message IntArrayOfArray {\n" +
                "  repeated IntArray values = 1;\n" +
                "}\n" +
                "message TripleNestedRecord {\n" +
                "  int32 id = 1;\n" +
                "  repeated IntArrayOfArray nestedInts = 2;\n" +
                "}\n";
    }

    private Supplier<String> oneofTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"textValue\" TEXT, " +
                "\"intValue\" INTEGER, " +
                "\"doubleValue\" DOUBLE PRECISION " +
                ");";
    }

    private Supplier<String> oneofProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message OneofRecord {\n" +
                "  int32 id = 1;\n" +
                "  oneof value {\n" +
                "    string textValue = 2;\n" +
                "    int32 intValue = 3;\n" +
                "    double doubleValue = 4;\n" +
                "  }\n" +
                "}\n";
    }

    private Supplier<String> oneofOfArrayTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"textArray\" ARRAY(TEXT), " +
                "\"intArray\" ARRAY(INTEGER) " +
                ");";
    }

    private Supplier<String> oneofOfArrayProtobufSchema() {
        // Protobuf doesn't allow `repeated` directly inside a `oneof`, so each branch is a
        // wrapper message. After flatten.unions=true, both branches become top-level fields whose
        // schema mirrors the wrapper (List<Struct{values: List<X>}> on the Connect side, which the
        // connector unwraps the same way as a regular `array(<scalar>)`).
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message StringArray {\n" +
                "  repeated string values = 1;\n" +
                "}\n" +
                "message IntArray {\n" +
                "  repeated int32 values = 1;\n" +
                "}\n" +
                "message OneofOfArrayRecord {\n" +
                "  int32 id = 1;\n" +
                "  oneof payload {\n" +
                "    StringArray textArray = 2;\n" +
                "    IntArray intArray = 3;\n" +
                "  }\n" +
                "}\n";
    }

    private Supplier<String> nestedOneofTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"nestedText\" TEXT, " +
                "\"nestedInt\" INTEGER, " +
                "\"flatBool\" BOOLEAN " +
                ");";
    }

    private Supplier<String> nestedOneofProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message InnerOneof {\n" +
                "  oneof inner {\n" +
                "    string nestedText = 1;\n" +
                "    int32 nestedInt = 2;\n" +
                "  }\n" +
                "}\n" +
                "message NestedOneofRecord {\n" +
                "  int32 id = 1;\n" +
                "  oneof outer {\n" +
                "    InnerOneof nested = 2;\n" +
                "    bool flatBool = 3;\n" +
                "  }\n" +
                "}\n";
    }

    private Supplier<String> nestedMessageStructTableSchema() {
        // `STRUCT(...)` is the Firebolt struct column type. The integration test relies on this
        // syntax being accepted by the engine; if Firebolt rejects the DDL the test will fail at
        // setup and surface a clear "STRUCT not supported by this Firebolt account" diagnostic
        // instead of a misleading converter-level error.
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"payload\" STRUCT(a TEXT, b INTEGER) " +
                ");";
    }

    private Supplier<String> nestedMessageProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message Inner {\n" +
                "  string a = 1;\n" +
                "  int32 b = 2;\n" +
                "}\n" +
                "message NestedMessageRecord {\n" +
                "  int32 id = 1;\n" +
                "  Inner payload = 2;\n" +
                "}\n";
    }

    // ---------------------------------------------------------------------------
    // Record construction
    // ---------------------------------------------------------------------------

    private List<DynamicMessage> generateTestRecords(Descriptor descriptor) {
        List<DynamicMessage> records = new ArrayList<>();

        // Record 1: typical values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 1)
                .setField(descriptor.findFieldByName("colBigint"), 1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), 1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), 1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "Basic Test Data")
                .setField(descriptor.findFieldByName("colDate"), "2024-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("hello".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "apple")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "banana")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "cherry")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 1)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 2)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 3)
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-01")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-02")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-03")
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 1.1f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 2.2f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 3.3f)
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "100.123456789")
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "200.987654321")
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 1.11111)
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 2.22222)
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 10, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 2, 13, 30, 10, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 10, 0)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 2, 13, 30, 10, 0)))
                .build());

        // Record 2: edge case / max values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 2)
                .setField(descriptor.findFieldByName("colBigint"), Long.MAX_VALUE)
                .setField(descriptor.findFieldByName("colNumeric"), "99999999999999999999999999999.999999999")
                .setField(descriptor.findFieldByName("colReal"), Float.MAX_VALUE)
                .setField(descriptor.findFieldByName("colDoublePrecision"), Double.MAX_VALUE)
                .setField(descriptor.findFieldByName("colBoolean"), false)
                .setField(descriptor.findFieldByName("colText"), "Edge Case Test Data with very long text that might exceed normal limits")
                .setField(descriptor.findFieldByName("colDate"), "2099-12-31")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999000000)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2099, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("edge_case_binary_data".getBytes()))
                .build());

        // Record 3: proto3 zero-defaults for scalar fields.
        //
        // Proto3 scalar defaults (0, false, "") arrive as zero values in the Connect Struct.
        // SchemaBasedRecordConverter passes them through as-is; the SQL/binary converters then
        // map them to their Firebolt equivalents (0 -> 0, false -> false, "" -> NULL for NUMERIC/DATE
        // since an empty string is not a valid NUMERIC/DATE literal). We therefore set colNumeric
        // and colDate explicitly to avoid converter rejection of "", and leave remaining string-typed
        // scalars (colText, colBytea) unset -- they arrive as "" / empty bytes and map to empty
        // string / empty bytea in Firebolt.
        //
        // Note that google.protobuf.Timestamp is a *message-type* field, not a scalar. Confluent's
        // ProtobufConverter behavior for unset proto3 message fields is configuration-dependent
        // (driven by flags like useOptionalForNullables / generateStructForNulls), so to keep this
        // test deterministic we explicitly set both Timestamp fields here. The dedicated
        // testOptionalProtobufFieldsCanBeAbsentForNullableColumns test exercises the absent-field
        // path via `optional` markers.
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 3)
                .setField(descriptor.findFieldByName("colNumeric"), "0")
                .setField(descriptor.findFieldByName("colDate"), "2000-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2010, 6, 15, 8, 30, 0, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2010, 6, 15, 8, 30, 0, 0, ZoneOffset.UTC)))
                .build());

        // Record 4: geographic sample data
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 4)
                .setField(descriptor.findFieldByName("colBigint"), 1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), 1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), 1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "San Francisco")
                .setField(descriptor.findFieldByName("colDate"), "2024-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 15, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("hello".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "San Francisco")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "New York")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "London")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 37)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 40)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 51)
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-01")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-02")
                .addRepeatedField(descriptor.findFieldByName("colArrayDate"), "2024-01-03")
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 37.7749f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 40.7128f)
                .addRepeatedField(descriptor.findFieldByName("colArrayReal"), 51.5074f)
                .build());

        // Record 5: negative / variety values
        records.add(DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("colInteger"), 5)
                .setField(descriptor.findFieldByName("colBigint"), -1000L)
                .setField(descriptor.findFieldByName("colNumeric"), "-12345678901234567890123456789.123456789")
                .setField(descriptor.findFieldByName("colReal"), -1.5f)
                .setField(descriptor.findFieldByName("colDoublePrecision"), -1.23456789)
                .setField(descriptor.findFieldByName("colBoolean"), true)
                .setField(descriptor.findFieldByName("colText"), "Variety Test Data with special characters: !@#$%^&*()")
                .setField(descriptor.findFieldByName("colDate"), "1970-01-01")
                .setField(descriptor.findFieldByName("colTimestamp"), toProtobufTimestamp(LocalDateTime.of(2000, 1, 1, 0, 0, 30, 0)))
                .setField(descriptor.findFieldByName("colTimestamptz"), toProtobufTimestamp(OffsetDateTime.of(2000, 1, 1, 0, 0, 35, 0, ZoneOffset.UTC)))
                .setField(descriptor.findFieldByName("colBytea"), ByteString.copyFrom("variety_binary_data".getBytes()))
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "apple")
                .addRepeatedField(descriptor.findFieldByName("colArrayText"), "banana")
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 1)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 2)
                .addRepeatedField(descriptor.findFieldByName("colArrayInt"), 3)
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "100.123456789")
                .addRepeatedField(descriptor.findFieldByName("colArrayNumeric"), "200.987654321")
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 1.11111)
                .addRepeatedField(descriptor.findFieldByName("colArrayDoublePrecision"), 2.22222)
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 1, 12, 0, 15, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamptz"),
                        toProtobufTimestamp(OffsetDateTime.of(2024, 1, 2, 13, 30, 20, 0, ZoneOffset.UTC)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 1, 12, 0, 25, 0)))
                .addRepeatedField(descriptor.findFieldByName("colArrayTimestamp"),
                        toProtobufTimestamp(LocalDateTime.of(2024, 1, 2, 13, 30, 25, 0)))
                .build());

        return records;
    }

    // ---------------------------------------------------------------------------
    // Verification
    // ---------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void verifyRecordsInFirebolt(List<DynamicMessage> expected, Descriptor descriptor) throws SQLException {
        String query =
                "SELECT \"colInteger\", \"colBigint\", \"colNumeric\", \"colReal\", \"colDoublePrecision\", " +
                "\"colBoolean\", \"colText\", \"colDate\", \"colTimestamp\", \"colTimestamptz\", \"colBytea\", " +
                "\"colArrayText\", \"colArrayInt\", \"colArrayDate\", \"colArrayReal\", " +
                "\"colArrayNumeric\", \"colArrayDoublePrecision\", \"colArrayTimestamptz\", \"colArrayTimestamp\" " +
                "FROM \"" + TABLE_NAME + "\" ORDER BY \"colInteger\"";

        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(query)) {
            int idx = 0;
            while (rs.next()) {
                assertTrue(idx < expected.size(), "More records in DB than expected");
                DynamicMessage rec = expected.get(idx);

                // Scalar fields
                assertEquals(rec.getField(descriptor.findFieldByName("colInteger")), rs.getInt("colInteger"),
                        "colInteger mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colBigint")), rs.getObject("colBigint", Long.class),
                        "colBigint mismatch at " + idx);

                // NUMERIC stored as string in proto; Firebolt returns BigDecimal
                String expectedNumeric = (String) rec.getField(descriptor.findFieldByName("colNumeric"));
                BigDecimal actualNumeric = rs.getBigDecimal("colNumeric");
                if (expectedNumeric != null && !expectedNumeric.isEmpty()) {
                    assertNotNull(actualNumeric, "colNumeric should not be null at " + idx);
                    assertEquals(0, new BigDecimal(expectedNumeric).compareTo(actualNumeric),
                            "colNumeric mismatch at " + idx + ": expected " + expectedNumeric + " got " + actualNumeric);
                } else {
                    assertNull(actualNumeric, "colNumeric should be null at " + idx);
                }

                assertEquals(rec.getField(descriptor.findFieldByName("colReal")), rs.getObject("colReal", Float.class),
                        "colReal mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colDoublePrecision")), rs.getObject("colDoublePrecision", Double.class),
                        "colDoublePrecision mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colBoolean")), rs.getObject("colBoolean", Boolean.class),
                        "colBoolean mismatch at " + idx);
                assertEquals(rec.getField(descriptor.findFieldByName("colText")), rs.getString("colText"),
                        "colText mismatch at " + idx);

                // DATE: stored as ISO string in proto, verify as date
                String expectedDate = (String) rec.getField(descriptor.findFieldByName("colDate"));
                java.sql.Date actualDate = rs.getDate("colDate");
                if (expectedDate != null && !expectedDate.isEmpty()) {
                    assertNotNull(actualDate, "colDate should not be null at " + idx);
                    assertEquals(expectedDate, actualDate.toString(), "colDate mismatch at " + idx);
                } else {
                    assertNull(actualDate, "colDate should be null at " + idx);
                }

                // TIMESTAMP: A non-optional google.protobuf.Timestamp field that is not set on the
                // wire is reconstructed by ProtobufConverter as the default Timestamp (seconds=0,
                // nanos=0), which the connector's schema-based timestamp converter persists as
                // 1970-01-01 00:00:00 UTC, NOT as SQL NULL. This is the same behavior callers
                // would observe with any other proto3 scalar default (0, false, ""). To get a
                // SQL NULL the producer must mark the field as `optional` and leave it absent.
                // DynamicMessage.getField() on a message-type field returns DynamicMessage, not
                // com.google.protobuf.Timestamp — use extractInstant() to decode seconds/nanos.
                Instant expectedTsInstant = extractInstant(rec.getField(descriptor.findFieldByName("colTimestamp")));
                java.sql.Timestamp actualTs = rs.getTimestamp("colTimestamp");
                assertNotNull(actualTs, "colTimestamp should not be null at " + idx);
                assertEquals(expectedTsInstant.toEpochMilli(), actualTs.getTime(), "colTimestamp mismatch at " + idx);

                // TIMESTAMPTZ: same proto3-defaults reasoning as TIMESTAMP above.
                Instant expectedTstzInstant = extractInstant(rec.getField(descriptor.findFieldByName("colTimestamptz")));
                java.sql.Timestamp actualTstz = rs.getTimestamp("colTimestamptz");
                assertNotNull(actualTstz, "colTimestamptz should not be null at " + idx);
                assertEquals(expectedTstzInstant, actualTstz.toInstant(), "colTimestamptz mismatch at " + idx);

                // BYTEA: proto bytes → byte[]
                // Proto3 default ByteString.EMPTY may arrive as null or empty byte[] from Firebolt.
                ByteString expectedBytea = (ByteString) rec.getField(descriptor.findFieldByName("colBytea"));
                byte[] actualBytea = rs.getBytes("colBytea");
                if (expectedBytea != null && !expectedBytea.isEmpty()) {
                    assertNotNull(actualBytea, "colBytea should not be null at " + idx);
                    assertArrayEquals(expectedBytea.toByteArray(), actualBytea, "colBytea mismatch at " + idx);
                } else {
                    assertTrue(actualBytea == null || actualBytea.length == 0,
                            "colBytea should be null or empty at " + idx);
                }

                // Arrays
                List<String> expectedText = (List<String>) rec.getField(descriptor.findFieldByName("colArrayText"));
                verifyStringArray("colArrayText", expectedText, rs.getString("colArrayText"), idx);

                List<Integer> expectedInts = (List<Integer>) rec.getField(descriptor.findFieldByName("colArrayInt"));
                verifyIntArray("colArrayInt", expectedInts, rs.getString("colArrayInt"), idx);

                List<String> expectedDates = (List<String>) rec.getField(descriptor.findFieldByName("colArrayDate"));
                verifyStringArray("colArrayDate", expectedDates, rs.getString("colArrayDate"), idx);

                List<Float> expectedReals = (List<Float>) rec.getField(descriptor.findFieldByName("colArrayReal"));
                verifyFloatArray("colArrayReal", expectedReals, rs.getString("colArrayReal"), idx);

                List<String> expectedNumerics = (List<String>) rec.getField(descriptor.findFieldByName("colArrayNumeric"));
                verifyDecimalStringArray("colArrayNumeric", expectedNumerics, rs.getString("colArrayNumeric"), idx);

                List<Double> expectedDoubles = (List<Double>) rec.getField(descriptor.findFieldByName("colArrayDoublePrecision"));
                verifyDoubleArray("colArrayDoublePrecision", expectedDoubles, rs.getString("colArrayDoublePrecision"), idx);

                // DynamicMessage.getField() on repeated message fields returns List<DynamicMessage>
                @SuppressWarnings("unchecked")
                List<DynamicMessage> expectedTstzArr =
                        (List<DynamicMessage>) rec.getField(descriptor.findFieldByName("colArrayTimestamptz"));
                verifyTimestamptzArray("colArrayTimestamptz", expectedTstzArr, rs.getString("colArrayTimestamptz"), idx);

                @SuppressWarnings("unchecked")
                List<DynamicMessage> expectedTsArr =
                        (List<DynamicMessage>) rec.getField(descriptor.findFieldByName("colArrayTimestamp"));
                verifyTimestampArray("colArrayTimestamp", expectedTsArr, rs.getString("colArrayTimestamp"), idx);

                idx++;
            }
            assertEquals(expected.size(), idx, "Expected " + expected.size() + " records but found " + idx);
        }
    }

    // ---------------------------------------------------------------------------
    // Array verification helpers
    // ---------------------------------------------------------------------------

    /**
     * Proto3 empty repeated fields default to [] and are stored as {} (empty array) in Firebolt,
     * not NULL. Accept both null and empty-array when expected is empty.
     */
    private void verifyStringArray(String field, List<String> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actual = parsePostgreSQLArray(actualStr);
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyIntArray(String field, List<Integer> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Integer> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Integer.parseInt(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyFloatArray(String field, List<Float> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Float> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Float.parseFloat(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyDoubleArray(String field, List<Double> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<Double> actual = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : Double.parseDouble(s))
                .collect(Collectors.toList());
        assertEquals(expected, actual, field + " mismatch at " + idx);
    }

    private void verifyDecimalStringArray(String field, List<String> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<BigDecimal> actualDecimals = parsePostgreSQLArray(actualStr).stream()
                .map(s -> s == null ? null : new BigDecimal(s))
                .collect(Collectors.toList());
        List<BigDecimal> expectedDecimals = expected.stream()
                .map(s -> s == null ? null : new BigDecimal(s))
                .collect(Collectors.toList());
        assertEquals(expectedDecimals.size(), actualDecimals.size(), field + " size mismatch at " + idx);
        for (int i = 0; i < expectedDecimals.size(); i++) {
            assertEquals(0, expectedDecimals.get(i).compareTo(actualDecimals.get(i)),
                    field + " element " + i + " mismatch at " + idx);
        }
    }

    private void verifyTimestamptzArray(
            String field, List<DynamicMessage> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actualStrings = parsePostgreSQLArray(actualStr);
        List<Instant> actualInstants = actualStrings.stream()
                .map(s -> {
                    if (s == null) return null;
                    String normalized = s.replace(" ", "T");
                    if (normalized.matches(".*[+-]\\d{2}$")) normalized = normalized + ":00";
                    return OffsetDateTime.parse(normalized).toInstant();
                }).collect(Collectors.toList());
        List<Instant> expectedInstants = expected.stream()
                .map(this::extractInstant)
                .collect(Collectors.toList());
        assertEquals(expectedInstants, actualInstants, field + " mismatch at " + idx);
    }

    private void verifyTimestampArray(
            String field, List<DynamicMessage> expected, String actualStr, int idx) {
        if (expected == null || expected.isEmpty()) {
            assertNullOrEmptyArray(field, actualStr, idx);
            return;
        }
        assertNotNull(actualStr, field + " should not be null at " + idx);
        List<String> actualStrings = parsePostgreSQLArray(actualStr);
        List<LocalDateTime> actualLdts = actualStrings.stream()
                .map(s -> s == null ? null : LocalDateTime.parse(s.replace(" ", "T")))
                .collect(Collectors.toList());
        List<LocalDateTime> expectedLdts = expected.stream()
                .map(dm -> LocalDateTime.ofInstant(extractInstant(dm), ZoneOffset.UTC))
                .collect(Collectors.toList());
        assertEquals(expectedLdts.size(), actualLdts.size(), field + " size mismatch at " + idx);
        for (int i = 0; i < expectedLdts.size(); i++) {
            LocalDateTime exp = expectedLdts.get(i);
            LocalDateTime act = actualLdts.get(i);
            if (exp == null) {
                assertNull(act, field + " element " + i + " should be null at " + idx);
            } else {
                assertEquals(exp.getYear(), act.getYear(), field + " year mismatch at " + idx);
                assertEquals(exp.getMonth(), act.getMonth(), field + " month mismatch at " + idx);
                assertEquals(exp.getDayOfMonth(), act.getDayOfMonth(), field + " day mismatch at " + idx);
                assertEquals(exp.getHour(), act.getHour(), field + " hour mismatch at " + idx);
                assertEquals(exp.getMinute(), act.getMinute(), field + " minute mismatch at " + idx);
                assertEquals(exp.getSecond(), act.getSecond(), field + " second mismatch at " + idx);
            }
        }
    }

    /**
     * Asserts that a Firebolt array column is either SQL NULL (returned to the JDBC client as
     * the literal {@code "NULL"} or {@code null}) or an empty array (rendered as {@code {}}).
     * {@link #parsePostgreSQLArray(String)} returns {@code null} for the literal {@code "NULL"}
     * string, which previously caused a NullPointerException when the caller invoked
     * {@code .isEmpty()} on the result.
     */
    private void assertNullOrEmptyArray(String field, String actualStr, int idx) {
        if (actualStr == null) {
            return;
        }
        List<String> parsed = parsePostgreSQLArray(actualStr);
        assertTrue(parsed == null || parsed.isEmpty(),
                field + " should be null or empty at " + idx + " (got: " + actualStr + ")");
    }

    /**
     * Extracts an Instant from a google.protobuf.Timestamp field value returned by
     * DynamicMessage.getField().
     *
     * The runtime type depends on whether the field was explicitly set:
     *   SET   → com.google.protobuf.Timestamp  (compiled well-known type)
     *   UNSET → com.google.protobuf.DynamicMessage  (proto3 default empty message)
     *
     * Both cases must be handled; assuming either type exclusively causes a ClassCastException
     * on the other path.
     */
    private Instant extractInstant(Object timestampMsg) {
        if (timestampMsg instanceof com.google.protobuf.Timestamp) {
            com.google.protobuf.Timestamp ts = (com.google.protobuf.Timestamp) timestampMsg;
            return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        }
        // UNSET proto3 Timestamp field: DynamicMessage with seconds=0, nanos=0 (epoch)
        DynamicMessage dm = (DynamicMessage) timestampMsg;
        long seconds = (long) dm.getField(dm.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (int) dm.getField(dm.getDescriptorForType().findFieldByName("nanos"));
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /** Builds a google.protobuf.Timestamp from a LocalDateTime (treated as UTC). */
    private com.google.protobuf.Timestamp toProtobufTimestamp(LocalDateTime ldt) {
        Instant instant = ldt.toInstant(ZoneOffset.UTC);
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /** Builds a google.protobuf.Timestamp from an OffsetDateTime. */
    private com.google.protobuf.Timestamp toProtobufTimestamp(OffsetDateTime odt) {
        Instant instant = odt.toInstant();
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private DynamicMessage intArray(Descriptor descriptor, int... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (int value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    private DynamicMessage longArray(Descriptor descriptor, long... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (long value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    private DynamicMessage doubleArray(Descriptor descriptor, double... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (double value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    private DynamicMessage stringArray(Descriptor descriptor, String... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (String value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    private DynamicMessage boolArray(Descriptor descriptor, boolean... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (boolean value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    // ---------------------------------------------------------------------------
    // PostgreSQL array string parser (mirrors AllDataTypesAvroSchemaSerializerTest)
    // ---------------------------------------------------------------------------

    private List<String> parsePostgreSQLArray(String arrayString) {
        List<String> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return null;
        }
        String content = arrayString.substring(1, arrayString.length() - 1);
        if (content.trim().isEmpty()) {
            return result;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                result.add(parseElement(current.toString().trim()));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(parseElement(current.toString().trim()));
        return result;
    }

    private List<List<Integer>> parseNestedIntegerArray(String arrayString) {
        return parseNestedArray(arrayString, Integer::parseInt);
    }

    private List<List<Long>> parseNestedLongArray(String arrayString) {
        return parseNestedArray(arrayString, Long::parseLong);
    }

    private List<List<Double>> parseNestedDoubleArray(String arrayString) {
        return parseNestedArray(arrayString, Double::parseDouble);
    }

    private List<List<String>> parseNestedStringArray(String arrayString) {
        return parseNestedArray(arrayString, s -> s);
    }

    private List<List<Boolean>> parseNestedBooleanArray(String arrayString) {
        return parseNestedArray(arrayString, s -> {
            // Firebolt renders boolean arrays using lowercase t/true,f/false depending on
            // version; accept both formats so the test is resilient.
            String lower = s.toLowerCase();
            if (lower.equals("t") || lower.equals("true")) return Boolean.TRUE;
            if (lower.equals("f") || lower.equals("false")) return Boolean.FALSE;
            throw new IllegalArgumentException("Unrecognized boolean literal: " + s);
        });
    }

    /**
     * Parses a Firebolt-rendered triple-nested integer array such as
     * {@code {{{1,2},{3}},{{4,5,6}}}}. Returns an empty outer list for null / empty / "NULL"
     * input.
     */
    private List<List<List<Integer>>> parseTripleNestedIntegerArray(String arrayString) {
        List<List<List<Integer>>> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return result;
        }
        String content = arrayString.substring(1, arrayString.length() - 1);
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                if (depth++ > 0) {
                    current.append(c);
                }
            } else if (c == '}') {
                if (--depth == 0) {
                    result.add(parseNestedIntegerArray("{" + current + "}"));
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else if (depth > 0) {
                current.append(c);
            }
        }
        return result;
    }

    /**
     * Parses a Firebolt-rendered nested array string like {@code {{1,2},{3,4}}} into a
     * {@code List<List<T>>} via the supplied element parser. Returns an empty outer list when the
     * input is null/empty/{@code "NULL"} so callers can detect both SQL NULL and an empty array
     * uniformly.
     */
    private <T> List<List<T>> parseNestedArray(String arrayString, java.util.function.Function<String, T> elemParser) {
        List<List<T>> result = new ArrayList<>();
        if (arrayString == null || arrayString.trim().isEmpty() || arrayString.equals("NULL")) {
            return result;
        }

        String content = arrayString.substring(1, arrayString.length() - 1);
        StringBuilder current = new StringBuilder();
        int depth = 0;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                if (depth++ > 0) {
                    current.append(c);
                }
            } else if (c == '}') {
                if (--depth == 0) {
                    List<String> rawElements = parsePostgreSQLArray("{" + current + "}");
                    List<T> inner = rawElements == null
                            ? new ArrayList<>()
                            : rawElements.stream()
                                    .map(value -> value == null ? null : elemParser.apply(value))
                                    .collect(Collectors.toList());
                    result.add(inner);
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else if (depth > 0) {
                current.append(c);
            }
        }
        return result;
    }

    private String parseElement(String elem) {
        if (elem.equals("NULL")) return null;
        if (elem.startsWith("\"") && elem.endsWith("\"")) return elem.substring(1, elem.length() - 1);
        return elem;
    }
}

