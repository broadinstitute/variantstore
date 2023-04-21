package org.broadinstitute.gvs.azure.cosmos;

import java.util.NoSuchElementException;

public class CosmosEndpointAndKey {
    public String getEndpoint() {
        return endpoint;
    }

    public String getKey() {
        return key;
    }

    private final String endpoint;
    private final String key;

    private CosmosEndpointAndKey(String endpoint, String key) {
        this.endpoint = endpoint;
        this.key = key;
    }

    public static CosmosEndpointAndKey fromEnvironment() {
        try {
            return new CosmosEndpointAndKey(
                    System.getenv("COSMOS_ENDPOINT"), System.getenv("COSMOS_KEY"));
        } catch (NoSuchElementException e) {
            throw new RuntimeException("Error: " + e.getMessage() + " environment variable not set.", e);
        }
    }
}
