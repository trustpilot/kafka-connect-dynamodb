package com.trustpilot.connector.dynamodb.testcontainers;

import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.utility.DockerImageName;

public class MockServerContainerExt extends MockServerContainer {
    public MockServerContainerExt(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public String getInternalEndpoint() {
        return String.format("http://%s:%d", getNetworkAliases().get(0), getExposedPorts().get(0));
    }
}
