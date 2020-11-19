package com.trustpilot.connector.dynamodb.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int PORT = 8081;
    private static final String networkAlias = "schema-registry";

    public SchemaRegistryContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withExposedPorts(PORT);
    }

    public SchemaRegistryContainer withKafka(final KafkaContainer kafka) {
        withNetwork(kafka.getNetwork());
        withNetworkAliases(networkAlias);
        addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                String.format("%s:%d", kafka.getNetworkAliases().get(0), 9092));

        return this;
    }
}