package org.apache.cassandra.sidecar.common.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleStatusResponse {
    @JsonProperty("result")
    public final String result;

    @JsonCreator
    public SimpleStatusResponse(@JsonProperty("result") String result) {
        this.result = result;
    }
}
