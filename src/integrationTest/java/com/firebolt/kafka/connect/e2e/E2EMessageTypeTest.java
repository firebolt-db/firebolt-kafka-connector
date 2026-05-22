package com.firebolt.kafka.connect.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import com.firebolt.kafka.connect.utils.TestTag;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Parameterized E2E tests for the config matrix.
 *
 * By default only JSON is exercised (4 cells: 2 delivery × 2 ingestion).
 * Set {@code -De2e.fullMatrix=true} to run all 12 cells
 * (3 message types × 2 delivery × 2 ingestion).
 */
@Slf4j
@Tag(TestTag.E2E)
public class E2EMessageTypeTest {

    private final E2ETestHarness harness = new E2ETestHarness();

    /**
     * CI-default matrix: JSON only (4 cells).
     * For fast local iteration, pin to one cell with
     * {@code -De2e.singleCell=at_least_once-sql} (delivery-ingestion).
     */
    static Stream<Arguments> ciConfigMatrix() {
        String singleCell = System.getProperty("e2e.singleCell");
        List<Arguments> configs = new ArrayList<>();
        for (DeliveryMode deliveryMode : DeliveryMode.values()) {
            for (IngestionType ingestionType : IngestionType.values()) {
                if (singleCell != null
                        && !singleCell.equals(deliveryMode.getValue() + "-" + ingestionType.getValue())) {
                    continue;
                }
                E2ETestConfig config = E2ETestConfig.builder()
                        .messageType(E2EMessageType.JSON)
                        .deliveryMode(deliveryMode)
                        .ingestionType(ingestionType)
                        .build();
                configs.add(Arguments.of(config));
            }
        }
        return configs.stream();
    }

    /**
     * Full matrix: all message types (12 cells).
     * Activated with {@code -De2e.fullMatrix=true}.
     */
    static Stream<Arguments> fullConfigMatrix() {
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
    @MethodSource("ciConfigMatrix")
    void testIngestion(E2ETestConfig config) throws Exception {
        log.info("=== E2E Test: {} ===", config.label());
        harness.setup(config);
        harness.produceForDuration();
        harness.waitForIngestion();
        harness.validateRecordCount();
        harness.validateDataIntegrity();
        harness.writeBenchmarkResult();
    }

    @ParameterizedTest(name = "full: {0}")
    @MethodSource("fullConfigMatrix")
    void testFullMatrixIngestion(E2ETestConfig config) throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("e2e.fullMatrix"),
                "Full matrix disabled — set -De2e.fullMatrix=true to enable");
        log.info("=== E2E Full Matrix Test: {} ===", config.label());
        harness.setup(config);
        harness.produceForDuration();
        harness.waitForIngestion();
        harness.validateRecordCount();
        harness.validateDataIntegrity();
        harness.writeBenchmarkResult();
    }

    @AfterEach
    void tearDown() {
        harness.cleanup();
    }
}

