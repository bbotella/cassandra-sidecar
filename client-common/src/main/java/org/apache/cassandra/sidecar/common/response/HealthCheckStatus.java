package org.apache.cassandra.sidecar.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HealthCheckStatus {
    @JsonProperty("status")
    public final String status;

    @JsonCreator
    public HealthCheckStatus(@JsonProperty("status") String status) {
        this.status = status;
    }
}
