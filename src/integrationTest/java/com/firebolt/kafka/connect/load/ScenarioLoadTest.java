package com.firebolt.kafka.connect.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.clients.ConfluentResourceClient;
import com.firebolt.kafka.connect.load.messagegenerator.LoopingCsvFileMessageGenerator;
import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import com.firebolt.kafka.connect.load.publisher.JsonSchemaRegistryKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.JsonSchemalessKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.KafkaMessagePublisher;
import com.firebolt.kafka.connect.load.verifier.FireboltTableRecordVerifier;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScenarioLoadTest {

	// Static outbound endpoints (staging)
	private static final Set<String> STAGING_APIS = Set.of("id.staging.firebolt.io", "api.staging.firebolt.io");

	public static void main(String[] args) throws Exception {
		ConfluentCloudSettings confluentCloudSettings = confluentCloudSettings();
		FireboltSettings fireboltSettings = fireboltSettings();

		// Inputs
		String scenarioName = System.getProperty("loadtest.scenario.name", "scenario1");
		int messageCount = Integer.parseInt(System.getProperty("loadtest.message.count", "1000000"));
		String ingestionType = System.getProperty("loadtest.ingestion.type", "sql");
		String messageType = System.getProperty("loadtest.message.type", "json");
		int minFetchMegabytes = Integer.parseInt(System.getProperty("loadtest.fetch.min.megabytes", "20"));
		int maxWaitTimeMs = Integer.parseInt(System.getProperty("loadtest.fetch.max.wait.ms", "2000"));
		int maxPollRecords = Integer.parseInt(System.getProperty("loadtest.max.poll.records", "10000"));

		// Scenario resources
		String scenarioBaseDir = "src/integrationTest/resources/load/scenarios/" + scenarioName;
		String scenarioJsonPath = scenarioBaseDir + "/scenario.json";
		String tableSchemaPath = scenarioBaseDir + "/table-schema.txt";
		String csvMessagesPath = scenarioBaseDir + "/kafka-messages.csv";

		ScenarioConfig scenarioConfig = readScenarioConfig(scenarioJsonPath);
		boolean hasSchema = scenarioConfig.isHasSchema();

		String jsonSchemaRegistryDefinitionFilePath = null;
		if (hasSchema) {
			String candidateSchemaPath = scenarioBaseDir + "/schema-registry.txt";
			jsonSchemaRegistryDefinitionFilePath = requireFile(candidateSchemaPath, "json schema registry");
		}

		// Build publisher using CSV-backed message generator
		KafkaMessagePublisher<?> messagePublisher = createMessagePublisherFromCsv(
				csvMessagesPath,
				hasSchema,
				messageType,
				confluentCloudSettings
		);

		// Connector overrides
		Map<String, String> connectorOverrides = new HashMap<>();
		long minFetchBytes = (long) minFetchMegabytes * 1024 * 1024;
		connectorOverrides.put("consumer.override.fetch.min.bytes", String.valueOf(minFetchBytes));
		connectorOverrides.put("consumer.override.fetch.max.wait.ms", String.valueOf(maxWaitTimeMs));
		connectorOverrides.put("consumer.override.max.poll.records", String.valueOf(maxPollRecords));
		if ("binary".equalsIgnoreCase(ingestionType)) {
			connectorOverrides.put("ingestion.type", "binary");
		}

		String name = "scenario-" + scenarioName;

		// No-op verifier for generic CSV scenarios (row-count validation is handled by LoadTestRunner)
		FireboltTableRecordVerifier noopVerifier = (client, tableName) -> true;

		TestScenario testScenario = TestScenario.builder()
				.averageMessageSizeInBytes(0) // unknown; informational only
				.nrOfKafkaMessageToProduce(messageCount)
				.connectorName(name)
				.topicName(name)
				.fireboltIngestionWaitDuration(Duration.ofMinutes(60))
				.tableSchemaDefinitionFilePath(requireFile(tableSchemaPath, "table schema"))
				.jsonSchemaRegistryDefinitionFilePath(jsonSchemaRegistryDefinitionFilePath)
				.staticOutboundHostnames(STAGING_APIS)
				.confluentCloudSettings(confluentCloudSettings)
				.fireboltSettings(fireboltSettings)
				.connectorConfiguration(connectorOverrides)
				.deleteConnector(true)
				.deleteTable(true)
				.loadTestKafkaMessagePublisher(messagePublisher)
				.fireboltTableRecordVerifier(noopVerifier)
				.build();

		log.info("Running scenario '{}' with config:", scenarioName);
		log.info("  hasSchema          : {}", hasSchema);
		log.info("  messageCount       : {}", messageCount);
		log.info("  ingestionType      : {}", ingestionType);
		log.info("  messageType        : {}", messageType);
		log.info("  minFetchMegabytes  : {}", minFetchMegabytes);
		log.info("  maxWaitTimeMs      : {}", maxWaitTimeMs);
		log.info("  maxPollRecords     : {}", maxPollRecords);
		log.info("  tableSchemaPath    : {}", tableSchemaPath);
		log.info("  csvMessagesPath    : {}", csvMessagesPath);
		log.info("  schemaRegistryPath : {}", jsonSchemaRegistryDefinitionFilePath);

		LoadTestRunner runner = new LoadTestRunner(testScenario);
		LoadTestRunResult result = runner.run();
		log.info("Scenario '{}' results: {}", scenarioName, result);
	}

	private static KafkaMessagePublisher<?> createMessagePublisherFromCsv(
			String csvMessagesPath,
			boolean hasSchema,
			String messageType,
			ConfluentCloudSettings confluentCloudSettings
	) throws IOException {
		if (!"json".equalsIgnoreCase(messageType)) {
			throw new IllegalArgumentException("Currently we only support json message type");
		}

		try (ConfluentResourceClient confluentResourceClient = new ConfluentResourceClient(
				confluentCloudSettings.getCloudResourceApiKey(),
				confluentCloudSettings.getCloudResourceApiSecret())) {
			String schemaRegistryUrl = confluentResourceClient.getSchemaRegistryUrl(confluentCloudSettings.getEnvironmentId());
			String bootstrapServers = confluentResourceClient.getBootstrapServerUrl(
					confluentCloudSettings.getClusterId(),
					confluentCloudSettings.getEnvironmentId());

			MessageGenerator<?> messageGenerator = new LoopingCsvFileMessageGenerator(csvMessagesPath);

			if (hasSchema) {
				return new JsonSchemaRegistryKafkaMessagePublisher<>(
						bootstrapServers,
						confluentCloudSettings.getKafkaApiKey(),
						confluentCloudSettings.getKafkaApiSecret(),
						schemaRegistryUrl,
						confluentCloudSettings.getSchemaRegistryApiKey(),
						confluentCloudSettings.getSchemaRegistryApiSecret(),
						messageGenerator);
			} else {
				return new JsonSchemalessKafkaMessagePublisher(
						bootstrapServers,
						confluentCloudSettings.getKafkaApiKey(),
						confluentCloudSettings.getKafkaApiSecret(),
						messageGenerator);
			}
		}
	}

	private static ScenarioConfig readScenarioConfig(String scenarioJsonPath) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(new File(requireFile(scenarioJsonPath, "scenario json")), ScenarioConfig.class);
		} catch (IOException e) {
			throw new RuntimeException("Failed to read scenario config at: " + scenarioJsonPath, e);
		}
	}

	private static String requireFile(String path, String label) {
		File f = new File(path);
		if (!f.exists() || !f.isFile()) {
			throw new IllegalArgumentException("Could not find " + label + " file at: " + path);
		}
		return path;
	}

	private static FireboltSettings fireboltSettings() {
		return FireboltSettings.builder()
				.jdbcUrl(System.getProperty("firebolt.jdbc.url", "jdbc:firebolt:sink-connector-load-test?account=goprean-us-east&engine=sink_connector_load_test&env=staging"))
				.clientId(System.getProperty("firebolt.client.id"))
				.clientSecret(System.getProperty("firebolt.client.secret"))
				.build();
	}

	private static ConfluentCloudSettings confluentCloudSettings() {
		return ConfluentCloudSettings.builder()
				.environmentId(System.getProperty("confluent.environment.id"))
				.clusterId(System.getProperty("confluent.cluster.id"))
				.fireboltConnectorPluginId(System.getProperty("confluent.firebolt.connector.plugin.id"))
				.kafkaApiKey(System.getProperty("confluent.kafka.api.key"))
				.kafkaApiSecret(System.getProperty("confluent.kafka.api.secret"))
				.schemaRegistryApiKey(System.getProperty("confluent.schema.registry.api.key"))
				.schemaRegistryApiSecret(System.getProperty("confluent.schema.registry.api.secret"))
				.cloudResourceApiKey(System.getProperty("confluent.cloud.resource.api.key"))
				.cloudResourceApiSecret(System.getProperty("confluent.cloud.resource.api.secret"))
				.build();
	}

	@Getter
	private static class ScenarioConfig {
		private String description;
		private boolean hasSchema;
	}
}


