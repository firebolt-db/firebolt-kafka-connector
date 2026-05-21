package com.firebolt.kafka.connect.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.kafka.connect.PostProcessingConfig;
import com.firebolt.kafka.connect.clients.ConfluentResourceClient;
import com.firebolt.kafka.connect.config.ConnectorConfigDefinition;
import com.firebolt.kafka.connect.load.messagegenerator.LoopingCsvFileMessageGenerator;
import com.firebolt.kafka.connect.load.messagegenerator.MessageGenerator;
import com.firebolt.kafka.connect.load.publisher.JsonSchemaRegistryKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.JsonSchemalessKafkaMessagePublisher;
import com.firebolt.kafka.connect.load.publisher.KafkaMessagePublisher;
import com.firebolt.kafka.connect.load.verifier.FireboltTableRecordVerifier;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import static java.nio.file.Paths.*;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class ScenarioLoadTest {

	// Static outbound endpoints (staging)
	private static final Set<String> STAGING_APIS = Set.of("id.staging.firebolt.io", "api.staging.firebolt.io");
	private static final long ONE_MEGA_BYTE = 1024 * 1024;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		ConfluentCloudSettings confluentCloudSettings = confluentCloudSettings();
		FireboltSettings fireboltSettings = fireboltSettings();

		// Inputs
		String scenarioName = System.getProperty("loadtest.scenario.name", "scenario1");
		int messageCount = Integer.parseInt(System.getProperty("loadtest.message.count", "1000000"));
		String ingestionType = System.getProperty("loadtest.ingestion.type", "sql");
		MessageType messageType = MessageType.fromString(System.getProperty("loadtest.message.type", "json"));
		boolean continuousPublishing = Boolean.parseBoolean(System.getProperty("loadtest.continuous.publishing", "false"));
		int messageBatchSize = Integer.parseInt(System.getProperty("loadtest.message.batch.size", "0"));
		int minFetchMegabytes = Integer.parseInt(System.getProperty("loadtest.fetch.min.megabytes", "20"));
		int maxWaitTimeMs = Integer.parseInt(System.getProperty("loadtest.fetch.max.wait.ms", "2000"));
		int maxPollRecords = Integer.parseInt(System.getProperty("loadtest.max.poll.records", "10000"));
		int fireboltIngestionWaitMinutes = Integer.parseInt(System.getProperty("loadtest.firebolt.ingestion.wait.minutes", "60"));

		// Scenario resources
		String scenarioBaseDir = "src/integrationTest/resources/load/scenarios/" + scenarioName;
		String scenarioJsonPath = scenarioBaseDir + "/scenario.json";
		String tableSchemaPath = scenarioBaseDir + "/table-schema.txt";
		String csvMessagesPath = scenarioBaseDir + "/kafka-messages.csv";

		// in case the of the post-processing script, this is the destination table and the post-processing script
		String destinationTableSchemaPath = scenarioBaseDir + "/destination-table-schema.txt";
		String postProcessingScriptPath = scenarioBaseDir + "/post_processing_script.txt";

		ScenarioConfig scenarioConfig = readScenarioConfig(scenarioJsonPath);
		boolean hasSchema = scenarioConfig.isHasSchema();

		String schemaDefinitionPath = null;
		if (hasSchema) {
			schemaDefinitionPath = requireFile(scenarioBaseDir + "/schema-registry.txt", "schema registry");
		}

		// Build publisher using CSV-backed message generator
		KafkaMessagePublisher<?> messagePublisher = createMessagePublisherFromCsv(
				csvMessagesPath,
				hasSchema,
				messageType,
				confluentCloudSettings,
				continuousPublishing,
				messageBatchSize
		);

		// Connector overrides
		Map<String, String> connectorOverrides = new HashMap<>();
		long minFetchBytes = (long) minFetchMegabytes * 1024 * 1024;
		connectorOverrides.put("consumer.override.fetch.min.bytes", String.valueOf(minFetchBytes));
		connectorOverrides.put("consumer.override.fetch.max.bytes", String.valueOf(minFetchBytes + ONE_MEGA_BYTE));
		connectorOverrides.put("consumer.override.fetch.max.wait.ms", String.valueOf(maxWaitTimeMs));
		connectorOverrides.put("consumer.override.max.poll.records", String.valueOf(maxPollRecords));
		// since we only have one partition we can just set it to hte
		connectorOverrides.put("consumer.override.max.partition.fetch.bytes", String.valueOf(minFetchBytes));

		if ("binary".equalsIgnoreCase(ingestionType)) {
			connectorOverrides.put("ingestion.type", "binary");
		}

		boolean hasPostProcessingScript = scenarioConfig.isHasPostProcessingScript();
		if (hasPostProcessingScript) {
			Path basePath = Paths.get("src/integrationTest/resources").toAbsolutePath().normalize();
			Path resolved = basePath.resolve(postProcessingScriptPath).normalize();
			if (!resolved.startsWith(basePath)) {
				throw new IllegalArgumentException("Invalid post-processing script path: " + postProcessingScriptPath);
			}
			String postProcessingScript = new String(java.nio.file.Files.readAllBytes(resolved));

			connectorOverrides.put(ConnectorConfigDefinition.POST_PROCESSING_SCRIPT_CONFIG,
					preparePostProcessingScript(scenarioConfig.getTableName(), postProcessingScript));
		}

		String name = "ecosystem-load-test-scenario-" + scenarioName + "-" + RandomStringUtils.randomNumeric(2);

		// No-op verifier for generic CSV scenarios (row-count validation is handled by LoadTestRunner)
		FireboltTableRecordVerifier noopVerifier = (client, tableName) -> true;

		TestScenario.TestScenarioBuilder testScenarioBuilder = TestScenario.builder()
				.averageMessageSizeInBytes(0) // unknown; informational only
				.nrOfKafkaMessageToProduce(messageCount)
				.connectorName(name)
				.topicName(name)
				.tableName(scenarioConfig.getTableName())
				.fireboltIngestionWaitDuration(Duration.ofMinutes(fireboltIngestionWaitMinutes))
				.tableSchemaDefinitionFilePath(requireFile(tableSchemaPath, "table schema"))
				.schemaDefinitionPath(schemaDefinitionPath)
				.messageType(messageType)
				.staticOutboundHostnames(STAGING_APIS)
				.confluentCloudSettings(confluentCloudSettings)
				.fireboltSettings(fireboltSettings)
				.connectorConfiguration(connectorOverrides)
				.deleteConnector(true)
				.deleteTable(true)
				.loadTestKafkaMessagePublisher(messagePublisher)
				.fireboltTableRecordVerifier(noopVerifier);

		if (hasPostProcessingScript) {
			testScenarioBuilder.destinationTableSchemaDefinitionFilePath(requireFile(destinationTableSchemaPath, "destination table schema"));
			testScenarioBuilder.destinationTableName(scenarioConfig.getFinalDestinationTableName());
		}

		TestScenario testScenario = testScenarioBuilder.build();

		log.info("Running scenario '{}' with config:", scenarioName);
		log.info("  hasSchema          : {}", hasSchema);
		log.info("  messageCount       : {}", messageCount);
		log.info("  ingestionType      : {}", ingestionType);
		log.info("  messageType        : {}", messageType.getValue());
		log.info("  continuousPublishing : {}", continuousPublishing);
		log.info("  messageBatchSize   : {}", messageBatchSize);
		log.info("  fireboltIngestionWaitMinutes : {}", fireboltIngestionWaitMinutes);
		log.info("  minFetchMegabytes  : {}", minFetchMegabytes);
		log.info("  maxWaitTimeMs      : {}", maxWaitTimeMs);
		log.info("  maxPollRecords     : {}", maxPollRecords);
		log.info("  tableSchemaPath    : {}", tableSchemaPath);
		log.info("  csvMessagesPath    : {}", csvMessagesPath);
		log.info("  schemaRegistryPath : {}", schemaDefinitionPath);

		LoadTestRunner runner = new LoadTestRunner(testScenario);
		LoadTestRunResult result = runner.run();
		log.info("Scenario '{}' results: {}", scenarioName, result);
	}

	private static String preparePostProcessingScript(String tableName, String scriptPath) {
		PostProcessingConfig postProcessingConfig = new PostProcessingConfig(
				List.of(
						new PostProcessingConfig.Mapping(tableName, scriptPath, null)
				));
		try {
			return objectMapper.writeValueAsString(postProcessingConfig);
		} catch (JsonProcessingException e) {
			fail("Failed to serialize the post processing config");
			return null;
		}
	}

	private static KafkaMessagePublisher<?> createMessagePublisherFromCsv(
			String csvMessagesPath,
			boolean hasSchema,
			MessageType messageType,
			ConfluentCloudSettings confluentCloudSettings,
			boolean continuousPublishing,
			int messageBatchSize
	) throws IOException {
		if (messageType != MessageType.JSON) {
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
						messageGenerator,
						continuousPublishing,
						messageBatchSize);
			} else {
				return new JsonSchemalessKafkaMessagePublisher(
						bootstrapServers,
						confluentCloudSettings.getKafkaApiKey(),
						confluentCloudSettings.getKafkaApiSecret(),
						messageGenerator,
						continuousPublishing,
						messageBatchSize);
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
		private String tableName;
		private String finalDestinationTableName;
		private boolean hasSchema;
		private boolean hasPostProcessingScript;
	}
}


