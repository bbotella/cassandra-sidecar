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

package org.apache.cassandra.sidecar.handlers;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.models.OpenAPI;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.config.OpenApiConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Handler that serves the OpenAPI specification
 */
@Tag(name = "Documentation", description = "API documentation endpoints")
@Singleton
public class OpenApiHandler implements Handler<RoutingContext>
{
    private final OpenApiConfiguration openApiConfig;

    @Inject
    public OpenApiHandler(SidecarConfiguration sidecarConfiguration)
    {
        this.openApiConfig = sidecarConfiguration.openApiConfiguration();
    }

    @Operation(
        summary = "Get OpenAPI specification",
        description = "Returns the OpenAPI specification for this API in JSON format"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "OpenAPI specification retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = OpenAPI.class)
            )
        )
    })
    @Override
    public void handle(RoutingContext context)
    {
        OpenAPI openAPI = createOpenApiFromConfig(openApiConfig);
        String openApiJson = Json.pretty(openAPI);
        
        context.response()
               .putHeader("Content-Type", "application/json")
               .end(openApiJson);
    }
    
    /**
     * Creates an OpenAPI configuration from the given configuration
     */
    private static OpenAPI createOpenApiFromConfig(OpenApiConfiguration config)
    {
        OpenAPI openApi = new OpenAPI();
        
        // Set basic info
        io.swagger.v3.oas.models.info.Info info = new io.swagger.v3.oas.models.info.Info();
        info.setTitle(config.title());
        info.setDescription(config.description());
        info.setVersion(config.version());
        
        // Set license info
        io.swagger.v3.oas.models.info.License license = new io.swagger.v3.oas.models.info.License();
        license.setName(config.licenseName());
        license.setUrl(config.licenseUrl());
        info.setLicense(license);
        
        openApi.setInfo(info);
        
        // Set server info
        io.swagger.v3.oas.models.servers.Server server = new io.swagger.v3.oas.models.servers.Server();
        server.setUrl(config.serverUrl());
        server.setDescription(config.serverDescription());
        openApi.setServers(java.util.Collections.singletonList(server));
        
        return openApi;
    }
}
