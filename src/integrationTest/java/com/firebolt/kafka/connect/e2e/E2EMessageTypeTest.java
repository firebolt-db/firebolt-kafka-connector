package com.firebolt.kafka.connect.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import com.firebolt.kafka.connect.utils.TestTag;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Parameterized E2E tests exercising all 12 cells of the config matrix:
 * 3 message types (JSON, AVRO, PROTOBUF)
 * × 2 delivery modes (AT_LEAST_ONCE, EXACTLY_ONCE)
 * × 2 ingestion types (SQL, BINARY)
 *
 * Each test creates infrastructure, produces records, waits for ingestion,
 * validates correctness, and cleans up. Requires Docker Compose stack running.
 */
@Slf4j
@Tag(TestTag.E2E)
public class E2EMessageTypeTest {

    private final E2ETestHarness harness = new E2ETestHarness();

    /**
     * Generates all 12 config matrix combinations.
     */
    static Stream<Arguments> configMatrix() {
        List<Arguments> configs = new ArrayList<>();
        for (E2EMessageType messageType : E2EMessageType.values()) {
            for (DeliveryMode deliveryMode : DeliveryMode.values()) {
                for (IngestionType ingestionType : IngestionType.values()) {
                    E2ETestConfig config = E2ETestConfig.builder()
                            .messageType(messageType)
                            .deliveryMode(deliveryMode)
                            .ingestionType(ingestionType)
                            .build();
                    configs.add(Arguments.of(config));
                }
            }
        }
        return configs.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configMatrix")
    void testIngestion(E2ETestConfig config) throws Exception {
        log.info("=== E2E Test: {} ===", config.label());
        harness.setup(config);
        harness.produceRecords(config.getRecordCount());
        harness.waitForIngestion();
        harness.validateRecordCount();
        harness.validateDataIntegrity();
    }

    @AfterEach
    void tearDown() {
        harness.cleanup();
    }
}
