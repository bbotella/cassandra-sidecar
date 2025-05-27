package org.apache.cassandra.sidecar.handlers;

import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.response.HealthCheckStatus; // Import the HealthCheckStatus
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

import static org.apache.cassandra.sidecar.modules.HealthCheckModule.OK_STATUS; // OK_STATUS uses a Map

@Singleton
public class SidecarHealthHandler implements Handler<RoutingContext> {

    @Override
    @Operation(summary = "Sidecar Health Check", description = "Checks if the Sidecar instance itself is healthy.")
    @APIResponse(responseCode = "200", description = "Sidecar is healthy.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = HealthCheckStatus.class)))
    public void handle(RoutingContext context) {
        // OK_STATUS is Map.of("status", "OK")
        // For consistency with other health checks, we should return HealthCheckStatus
        context.json(new HealthCheckStatus("OK"));
    }
}
