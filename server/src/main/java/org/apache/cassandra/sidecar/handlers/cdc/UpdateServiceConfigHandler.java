/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.sidecar.handlers.cdc;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.Service;
import org.apache.cassandra.sidecar.common.request.data.ServiceConfigPayload;
import org.apache.cassandra.sidecar.db.ConfigAccessor;
import org.apache.cassandra.sidecar.db.ConfigAccessorFactory;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.Parameter;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Provides REST endpoint for updating CDC/Kafka configs.
 */
@Singleton
public class UpdateServiceConfigHandler implements Handler<RoutingContext>, AccessProtected
{
    private final ConfigAccessorFactory configAccessorFactory;
    private final ServiceConfigValidator serviceConfigValidator;

    @Inject
    public UpdateServiceConfigHandler(ConfigAccessorFactory configAccessorFactory, ServiceConfigValidator serviceConfigValidator)
    {
        this.configAccessorFactory = configAccessorFactory;
        this.serviceConfigValidator = serviceConfigValidator;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.CDC.toAuthorization());
    }

    @Override
    @Operation(summary = "Update Service Configuration",
               description = "Updates the configuration for a specific service (e.g., 'cdc', 'kafka'). The request body should contain the configuration key-value pairs under a 'config' field.")
    @Parameter(name = "service",
               in = ParameterIn.PATH,
               description = "The name of the service to configure (e.g., 'cdc', 'kafka').",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @RequestBody(description = "Service configuration payload.",
                 required = true,
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = ServiceConfigPayload.class)))
    @APIResponse(responseCode = "200",
                 description = "Successfully updated service configuration. The response body is the applied configuration.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = ServiceConfigPayload.class)))
    @APIResponse(responseCode = "400",
                 description = "Bad request, e.g., invalid service name, missing 'config' in payload, or invalid configuration structure.")
    @APIResponse(responseCode = "500",
                 description = "Failed to update service configuration.")
    public void handle(RoutingContext context)
    {
        String serviceName = context.pathParam(ConfigPayloadParams.SERVICE);
        Service service = serviceConfigValidator.validateAndGet(serviceName);

        JsonObject payload = context.body().asJsonObject();
        serviceConfigValidator.validatePayload(payload);
        serviceConfigValidator.validateConfig(payload);

        ConfigAccessor accessor = configAccessorFactory.configAccessor(service);
        Map<String, String> config = payload.getJsonObject(ConfigPayloadParams.CONFIG)
                                            .getMap()
                                            .entrySet()
                                            .stream()
                                            .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
        accessor.storeConfig(config);
        context.json(payload);
    }
}
