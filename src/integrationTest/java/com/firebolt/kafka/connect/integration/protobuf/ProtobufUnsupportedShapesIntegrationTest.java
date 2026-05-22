package com.firebolt.kafka.connect.integration.protobuf;

import com.firebolt.kafka.connect.utils.TestTag;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Negative integration tests for Protobuf shapes that the connector currently does <em>not</em>
 * support end-to-end. These tests live in their own class so the supported-path
 * {@link ProtobufAllDataTypesSerializerTest} stays focused on what works today, and so the
 * unsupported scenarios remain clearly grouped with their TODO references.
 *
 * <p>Each test sends Protobuf data through the connector with {@code errors.tolerance=all} +
 * a DLQ topic and asserts:
 * <ul>
 *   <li>the Firebolt target table receives <em>zero</em> rows (the connector must not silently
 *       persist malformed values), and</li>
 *   <li>at least one record lands in the DLQ.</li>
 * </ul>
 *
 * <p>When connector support for one of the shapes lands, the corresponding test should flip from
 * "expect DLQ" to "expect successful ingestion".
 *
 * <h2>TODO: serialization / deserialization architecture re-evaluation</h2>
 * The cases captured here all stem from the schema-based serialization layer not yet handling
 * Connect Struct values nor deeper-than-two array nesting beyond the bespoke
 * {@code array(array(integer))} path. A follow-up PR will revisit the overall
 * serialization/deserialization architecture; specifically the connector should grow:
 * <ul>
 *   <li>A {@code SchemaStructDataTypeConverter} so plain (non-{@code oneof}) Protobuf nested
 *       messages can ingest into Firebolt {@code STRUCT} columns. Once Firebolt ships
 *       {@code VARIANT}, the converter should also cover that.</li>
 *   <li>Generalised nested-array support for any depth and any inner scalar type
 *       (currently only {@code array(array(integer))} is wired up).</li>
 * </ul>
 *
 * <h2>What does work today</h2>
 * Confluent's {@code ProtobufConverter} flattens {@code oneof} branches into top-level Connect
 * fields by default (it adds an {@code <oneof>_0} discriminator that the connector silently
 * ignores). As a result <em>{@code oneof} of scalars, {@code oneof} of arrays, and nested
 * {@code oneof}s all round-trip end-to-end</em> through the existing converter as long as each
 * branch maps to a regular scalar / array Firebolt column. Positive coverage for those shapes
 * lives in {@link ProtobufAllDataTypesSerializerTest}.
 */
@Slf4j
@Tag(TestTag.SERIALIZATION)
@Tag(TestTag.CONNECTOR)
@org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable(
        named = "KAFKA_CONNECT_VERSION",
        matches = "4\\.0",
        disabledReason = "KC 4.0 image has incompatible protobuf-java versions on the classpath; see ProtobufBaseIntegrationTest")
public class ProtobufUnsupportedShapesIntegrationTest extends ProtobufBaseIntegrationTest {

    private static final String TABLE_NAME = "protobuf_unsupported_shapes_table";
    private static final String TOPIC_NAME = "protobuf-unsupported-shapes-topic";
    private static final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private String dlqTopicName;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("protobuf-unsupported-shapes");
        dlqTopicName = "dlq-protobuf-unsupported-" + UUID.randomUUID();
        createKafkaTopic(dlqTopicName);
    }

    @AfterEach
    protected void tearDown() {
        cleanupProtobufTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        if (dlqTopicName != null) {
            safelyDeleteKafkaTopic(dlqTopicName);
        }
        super.tearDown();
    }

    /**
     * <strong>Triple-nested arrays.</strong> The schema-based array converter only special-cases
     * {@code array(array(integer))}; deeper nesting falls through to the 1D path which cannot
     * unwrap the Protobuf wrapper Struct hierarchy.
     *
     * <p>TODO: extend {@code SchemaArrayDataTypeConverter} to recursively unwrap nested
     * {@code List}/{@code Struct} layers to any depth so triple- and quadruple-nested arrays can
     * round-trip. See {@code ProtobufUnsupportedShapesIntegrationTest} class-level TODO.
     */
    @Test
    void tripleNestedArrayProtobufFailsAndRoutesToDlq() throws Exception {
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                tripleNestedArrayTableSchema(), tripleNestedArrayProtobufSchema(), dlqOverride());

        ProtobufSchema parsedSchema = new ProtobufSchema(tripleNestedArrayProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("TripleNestedRecord");
        Descriptor innerArr = fileDescriptor.findMessageTypeByName("IntArray");
        Descriptor middleArr = fileDescriptor.findMessageTypeByName("IntArrayOfArray");

        DynamicMessage record = DynamicMessage.newBuilder(recordDescriptor)
                .setField(recordDescriptor.findFieldByName("id"), 1)
                .addRepeatedField(recordDescriptor.findFieldByName("nestedInts"),
                        DynamicMessage.newBuilder(middleArr)
                                .addRepeatedField(middleArr.findFieldByName("values"), intArrayMsg(innerArr, 1, 2))
                                .build())
                .build();

        runAndAssertDlq(List.of(record), recordDescriptor, "id");
    }

    /**
     * <strong>Nested array with a non-integer inner type.</strong> Today the converter only wires
     * up the wrapper-Struct unwrap path for {@code array(array(integer))}; an
     * {@code array(array(text))} column with Protobuf wrapper messages slips through to the 1D
     * path and corrupts/raises.
     *
     * <p>TODO: generalise nested-array detection to any inner scalar type.
     */
    @Test
    void nestedArrayWithNonIntegerInnerTypeFailsAndRoutesToDlq() throws Exception {
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedTextArrayTableSchema(), nestedTextArrayProtobufSchema(), dlqOverride());

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedTextArrayProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("NestedTextArrayRecord");
        Descriptor stringArrayDescriptor = fileDescriptor.findMessageTypeByName("StringArray");

        DynamicMessage record = DynamicMessage.newBuilder(recordDescriptor)
                .setField(recordDescriptor.findFieldByName("id"), 1)
                .addRepeatedField(recordDescriptor.findFieldByName("nestedTexts"),
                        stringArrayMsg(stringArrayDescriptor, "alpha", "beta"))
                .build();

        runAndAssertDlq(List.of(record), recordDescriptor, "id");
    }

    /**
     * <strong>Plain (non-{@code oneof}) Protobuf nested message.</strong> Confluent's
     * {@code ProtobufConverter} surfaces the inner message as a Connect Struct field, which
     * the connector currently has no SQL-side handler for (no {@code SchemaStructDataTypeConverter}
     * wired up in {@code SchemaColumnTypeConverterFactory}).
     *
     * <p>TODO: implement STRUCT support so this test flips to a positive ingestion assertion.
     */
    @Test
    void nestedProtobufMessageTargetingFireboltStructFailsAndRoutesToDlq() throws Exception {
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedMessageStructTableSchema(), nestedMessageProtobufSchema(), dlqOverride());

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedMessageProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor recordDescriptor = fileDescriptor.findMessageTypeByName("NestedMessageRecord");
        Descriptor inner = fileDescriptor.findMessageTypeByName("Inner");

        DynamicMessage record = DynamicMessage.newBuilder(recordDescriptor)
                .setField(recordDescriptor.findFieldByName("id"), 1)
                .setField(recordDescriptor.findFieldByName("payload"),
                        DynamicMessage.newBuilder(inner)
                                .setField(inner.findFieldByName("a"), "x")
                                .setField(inner.findFieldByName("b"), 7)
                                .build())
                .build();

        runAndAssertDlq(List.of(record), recordDescriptor, "id");
    }

    /**
     * <strong>{@code oneof} without {@code flatten.unions=true}.</strong> By default Confluent's
     * {@code ProtobufConverter} wraps {@code oneof} members in a sub-Struct named after the
     * {@code oneof}. The connector has no SQL-side STRUCT handler so the sub-Struct field is
     * silently dropped (only the {@code id} column lands), losing all branch data without a
     * clear error.
     *
     * <p>This is the worst kind of negative behaviour — silent data loss. It is documented here
     * so that:
     * <ol>
     *   <li>Users running with default Confluent settings see this assertion break the moment
     *       a code change starts surfacing the data correctly (good: flip to a positive test).</li>
     *   <li>Until the architectural rework lands, users are funnelled toward the
     *       {@code value.converter.flatten.unions=true} setting which is exercised positively
     *       by {@code ProtobufAllDataTypesSerializerTest#testOneofOfScalarsProtobufFlattensToColumns}.</li>
     * </ol>
     *
     * <p>TODO: detect Connect Struct values targeting non-STRUCT columns at conversion time and
     * raise a {@code ColumnConversionFailedException} with a message recommending
     * {@code flatten.unions=true}, instead of silently dropping the data.
     */
    @Test
    void oneofProtobufWithoutFlattenUnionsSilentlyDropsBranchData() throws Exception {
        // Note: NO `flatten.unions=true` override -- this test exercises the default Confluent
        // configuration where the oneof becomes a sub-Struct field that the connector cannot
        // handle.
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                oneofTableSchema(), oneofProtobufSchema(), Map.of("ingestion.type", "sql"));

        ProtobufSchema parsedSchema = new ProtobufSchema(oneofProtobufSchema().get());
        Descriptor descriptor = parsedSchema.toDescriptor().getFile().findMessageTypeByName("OneofRecord");

        DynamicMessage textRecord = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .setField(descriptor.findFieldByName("textValue"), "hello")
                .build();
        DynamicMessage intRecord = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 2)
                .setField(descriptor.findFieldByName("intValue"), 42)
                .build();

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : List.of(textRecord, intRecord)) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        // The connector inserts the rows (without flagging the oneof drop) -- that's the silent
        // data loss being documented.
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> fireboltDefaultDbClient.countRows(TABLE_NAME) >= 2);

        try (java.sql.ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"textValue\", \"intValue\", \"doubleValue\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(null, rs.getString("textValue"),
                    "TODO: oneof branch data silently dropped without flatten.unions=true; see class-level Javadoc");
            assertEquals(null, rs.getObject("intValue"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(null, rs.getString("textValue"));
            assertEquals(null, rs.getObject("intValue"),
                    "TODO: oneof branch data silently dropped without flatten.unions=true; see class-level Javadoc");
        }
    }

    /**
     * Schema mirror of {@code ProtobufAllDataTypesSerializerTest#oneofTableSchema}. Kept here so
     * the negative test stays self-contained -- the assertion compares the silent-drop default
     * behaviour against the positive {@code flatten.unions=true} ingestion that the all-data-types
     * test exercises.
     */
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

    /**
     * <strong>Nested {@code oneof} with message-type branches.</strong>
     * {@code value.converter.flatten.unions=true} flattens the <em>top-level</em>
     * {@code oneof} into per-branch top-level Connect fields, but if a branch is itself a
     * <em>message</em> (here, the inner {@code oneof} wrapper), that message stays as a Connect
     * Struct field. The connector silently drops the Struct, losing all branch data inside the
     * inner {@code oneof}.
     *
     * <p>Scalar branches at the outer level (e.g., {@code flatBool}) <em>do</em> flatten because
     * they aren't message-typed; the test asserts that asymmetry — outer scalar lands, inner
     * branch payload disappears.
     *
     * <p>TODO: support flattening of nested message-type branches, or at minimum surface a clear
     * conversion error when a Struct value targets a non-STRUCT column.
     */
    @Test
    void nestedOneofWithMessageBranchesSilentlyDropsInnerData() throws Exception {
        Map<String, String> override = new java.util.HashMap<>();
        override.put("ingestion.type", "sql");
        override.put("value.converter.flatten.unions", "true");
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                nestedOneofTableSchema(), nestedOneofProtobufSchema(), override);

        ProtobufSchema parsedSchema = new ProtobufSchema(nestedOneofProtobufSchema().get());
        FileDescriptor fileDescriptor = parsedSchema.toDescriptor().getFile();
        Descriptor descriptor = fileDescriptor.findMessageTypeByName("NestedOneofRecord");
        Descriptor inner = fileDescriptor.findMessageTypeByName("InnerOneof");

        List<DynamicMessage> records = List.of(
                // Outer branch is the message-typed `nested` -> dropped; nestedText/nestedInt
                // remain NULL in Firebolt despite being set on the wire.
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 1)
                        .setField(descriptor.findFieldByName("nested"),
                                DynamicMessage.newBuilder(inner)
                                        .setField(inner.findFieldByName("nestedText"), "deep")
                                        .build())
                        .build(),
                // Outer branch is the scalar `flatBool` -> flattens to its column.
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 2)
                        .setField(descriptor.findFieldByName("flatBool"), true)
                        .build());

        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName("id"))), record)).get();
            }
            producer.flush();
        }

        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> fireboltDefaultDbClient.countRows(TABLE_NAME) >= 2);

        try (java.sql.ResultSet rs = fireboltDefaultDbClient.executeQuery(
                "SELECT \"id\", \"nestedText\", \"nestedInt\", \"flatBool\" FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(null, rs.getString("nestedText"),
                    "TODO: nested oneof branch (message-typed) is silently dropped; see class-level Javadoc");
            assertEquals(null, rs.getObject("nestedInt"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            // Scalar branch at the outer level flattens correctly even when an inner oneof exists.
            assertEquals(Boolean.TRUE, rs.getObject("flatBool", Boolean.class));
        }
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

    /**
     * <strong>Absent optional field targeting a NOT NULL Firebolt column.</strong> Sending a
     * Protobuf record with the {@code optional} field unset for a {@code NOT NULL} column must
     * not silently succeed: the connector forwards SQL NULL for the column, the database rejects
     * the row, and the failed record must end up in the DLQ rather than the target table.
     *
     * <p>This is not a "missing connector feature" -- it documents the connector's correct
     * NOT NULL semantics from the producer's point of view. Pinned here next to the other
     * negative tests so the expected-failure paths stay grouped.
     */
    @Test
    void absentOptionalProtobufFieldFailsForNotNullFireboltColumnAndRoutesToDlq() throws Exception {
        setupProtobufTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT,
                notNullTableSchema(), notNullOptionalProtobufSchema(), dlqOverride());

        ProtobufSchema parsedSchema = new ProtobufSchema(notNullOptionalProtobufSchema().get());
        Descriptor descriptor = parsedSchema.toDescriptor().getFile().findMessageTypeByName("OptionalFieldsRecord");

        // Both records omit the optional fields that map to NOT NULL columns. With at least two
        // records we exercise both the "drop column from INSERT" path (when ALL records have a
        // column null) and the per-row setNull path (when some records provide it).
        DynamicMessage r1 = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .build();
        DynamicMessage r2 = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 2)
                .build();

        runAndAssertDlq(List.of(r1, r2), descriptor, "id");
    }

    // ---------------------------------------------------------------------------
    // Shared infrastructure
    // ---------------------------------------------------------------------------

    private Map<String, String> dlqOverride() {
        return Map.of(
                "errors.tolerance", "all",
                "errors.deadletterqueue.topic.name", dlqTopicName,
                "errors.deadletterqueue.context.headers.enable", "true",
                "ingestion.type", "sql"
        );
    }

    private void runAndAssertDlq(List<DynamicMessage> records, Descriptor descriptor, String idField) throws Exception {
        try (Producer<String, DynamicMessage> producer = initializeProtobufProducer();
             KafkaConsumer<String, byte[]> dlqConsumer = createDlqConsumer()) {
            for (DynamicMessage record : records) {
                producer.send(new ProducerRecord<>(TOPIC_NAME,
                        String.valueOf(record.getField(descriptor.findFieldByName(idField))), record)).get();
            }
            producer.flush();

            int dlqMessages = 0;
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (dlqMessages < 1 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> polled = dlqConsumer.poll(Duration.ofSeconds(5));
                dlqMessages += polled.count();
            }

            assertEquals(0, fireboltDefaultDbClient.countRows(TABLE_NAME),
                    "Unsupported Protobuf shape must not land in Firebolt -- if rows appear here, " +
                            "support for the shape has likely landed and this test should be flipped " +
                            "to a positive ingestion assertion.");
            assertTrue(dlqMessages >= 1,
                    "Expected at least one record in DLQ for the unsupported Protobuf shape, got " + dlqMessages);
        }
    }

    private KafkaConsumer<String, byte[]> createDlqConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-unsupported-dlq-" + UUID.randomUUID());
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

    private DynamicMessage intArrayMsg(Descriptor descriptor, int... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (int value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    private DynamicMessage stringArrayMsg(Descriptor descriptor, String... values) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (String value : values) {
            builder.addRepeatedField(descriptor.findFieldByName("values"), value);
        }
        return builder.build();
    }

    // ---------------------------------------------------------------------------
    // Per-test schemas
    // ---------------------------------------------------------------------------

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

    private Supplier<String> nestedTextArrayTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"nestedTexts\" ARRAY(ARRAY(TEXT)) " +
                ");";
    }

    private Supplier<String> nestedTextArrayProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message StringArray {\n" +
                "  repeated string values = 1;\n" +
                "}\n" +
                "message NestedTextArrayRecord {\n" +
                "  int32 id = 1;\n" +
                "  repeated StringArray nestedTexts = 2;\n" +
                "}\n";
    }

    private Supplier<String> nestedMessageStructTableSchema() {
        // STRUCT(...) is the Firebolt struct column DDL. If the engine rejects it, the test will
        // fail at table creation rather than reaching the converter rejection path; that's the
        // correct failure surface to expose for this test.
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

    private Supplier<String> notNullTableSchema() {
        return () -> "CREATE TABLE \"%s\" (" +
                "\"id\" INTEGER NOT NULL, " +
                "\"optionalText\" TEXT NOT NULL, " +
                "\"optionalNumeric\" NUMERIC(38,9) NOT NULL " +
                ");";
    }

    private Supplier<String> notNullOptionalProtobufSchema() {
        return () ->
                "syntax = \"proto3\";\n" +
                "package com.firebolt.kafka.connect.integration.protobuf;\n" +
                "message OptionalFieldsRecord {\n" +
                "  int32 id = 1;\n" +
                "  optional string optionalText = 2;\n" +
                "  optional string optionalNumeric = 3;\n" +
                "}\n";
    }

}
