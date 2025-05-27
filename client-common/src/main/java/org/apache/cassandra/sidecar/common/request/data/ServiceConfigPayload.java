package org.apache.cassandra.sidecar.common.request.data;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServiceConfigPayload {
    @JsonProperty("config")
    public final Map<String, String> config;

    @JsonCreator
    public ServiceConfigPayload(@JsonProperty("config") Map<String, String> config) {
        this.config = config;
    }
}
