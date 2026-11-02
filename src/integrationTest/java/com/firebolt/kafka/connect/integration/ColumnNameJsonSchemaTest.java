package com.firebolt.kafka.connect.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.firebolt.kafka.connect.utils.TestTag;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Field names that are not Avro identifiers (dashes, dots, spaces, non-ASCII) through a schema-carrying
 * converter. Such records travel as Avro, where Confluent's AvroData has to scrub the names; the
 * connector writes the original names into the file header so {@code INSERT … BY NAME} still matches
 * the columns — including inside a nested STRUCT column.
 */
@Slf4j
@Tag(TestTag.CONNECTOR)
public class ColumnNameJsonSchemaTest extends SchemaBaseIntegrationTest {

    private static final String JSON_SCHEMA = "{"
            + "\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Names\",\"type\":\"object\","
            + "\"additionalProperties\":false,"
            + "\"properties\":{"
            + "\"id\":{\"type\":\"integer\"},"
            + "\"col-with-dashes\":{\"type\":\"string\"},"
            + "\"col.with.dots\":{\"type\":\"string\"},"
            + "\"col with spaces\":{\"type\":\"string\"},"
            + "\"über\":{\"type\":\"string\"},"
            + "\"nested struct\":{\"type\":\"object\",\"additionalProperties\":false,"
            + "\"properties\":{\"a-b\":{\"type\":\"string\"}},\"required\":[\"a-b\"]}},"
            + "\"required\":[\"id\",\"col-with-dashes\",\"col.with.dots\",\"col with spaces\",\"über\",\"nested struct\"]}";

    private final String TABLE_NAME = generateTableName("column_name_json_schema");
    private final String TOPIC_NAME = generateTopicName("column-name-json-schema");
    private final String SCHEMA_SUBJECT = TOPIC_NAME + "-value";

    private Producer<String, Object> producer;

    @BeforeEach
    protected void setUp(TestInfo testInfo) {
        super.setUp(testInfo);
        generateUniqueConnectorName("column-name-json-schema");
    }

    @AfterEach
    protected void tearDown() {
        if (producer != null) {
            producer.close();
        }
        cleanupTestResources(TABLE_NAME, TOPIC_NAME, SCHEMA_SUBJECT);
        super.tearDown();
    }

    @Test
    void nonAvroIdentifierFieldNamesMatchTheirColumns() throws Exception {
        setupTestResources(TOPIC_NAME, TABLE_NAME, SCHEMA_SUBJECT, () -> "CREATE TABLE \"%s\" ("
                        + "\"id\" BIGINT NOT NULL, \"col-with-dashes\" TEXT, \"col.with.dots\" TEXT, "
                        + "\"col with spaces\" TEXT, \"über\" TEXT, \"nested struct\" STRUCT(\"a-b\" TEXT))",
                () -> JSON_SCHEMA, Map.of("errors.tolerance", "none"));
        producer = initializeJsonProducer();

        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", 1);
        value.put("col-with-dashes", "d");
        value.put("col.with.dots", "p");
        value.put("col with spaces", "s");
        value.put("über", "u");
        value.put("nested struct", Map.of("a-b", "n"));
        producer.send(new ProducerRecord<>(TOPIC_NAME, "k", value)).get();
        producer.flush();

        waitForDataInFirebolt(TABLE_NAME, 1, Duration.ofSeconds(60));
        try (ResultSet rs = fireboltDefaultDbClient.executeQuery(String.format(
                "SELECT \"col-with-dashes\", \"col.with.dots\", \"col with spaces\", \"über\", "
                        + "\"nested struct\".\"a-b\" AS nested FROM \"%s\"", TABLE_NAME))) {
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals("p", rs.getString(2));
            assertEquals("s", rs.getString(3));
            assertEquals("u", rs.getString(4));
            assertEquals("n", rs.getString("nested"));
        }
    }
}
