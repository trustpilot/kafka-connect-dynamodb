package com.trustpilot.connector.dynamodb.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class DynamoDBContainer extends GenericContainer<DynamoDBContainer> {

    private static final int PORT = 8000;
    private static final String networkAlias = "dynamodb";


    public DynamoDBContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        this.withExposedPorts(PORT);
        this.withNetworkAliases(networkAlias);
    }

    public String getEndpoint() {
        return String.format("http://%s:%d", getHost(), getMappedPort(PORT));
    }

    public String getInternalEndpoint() {
        return String.format("http://%s:%d", getNetworkAliases().get(0), getExposedPorts().get(0));
    }
}
