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

package org.apache.cassandra.sidecar.modules;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.ProvidesIntoMap;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.handlers.OpenApiHandler;
import org.apache.cassandra.sidecar.handlers.SwaggerUIHandler;
import org.apache.cassandra.sidecar.handlers.WebJarHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Module for OpenAPI documentation
 */
@Path("/")
public class OpenApiModule extends AbstractModule
{
    @GET
    @Path(ApiEndpointsV1.OPENAPI_ROUTE)
    @Operation(summary = "Get OpenAPI specification",
               description = "Returns the OpenAPI specification for the Cassandra Sidecar API")
    @APIResponse(description = "OpenAPI specification retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.OpenApiRouteKey.class)
    VertxRoute openApiRoute(RouteBuilder.Factory factory, OpenApiHandler openApiHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(openApiHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.OPENAPI_YAML_ROUTE)
    @Operation(summary = "Get OpenAPI specification in YAML",
               description = "Returns the OpenAPI specification for the Cassandra Sidecar API in YAML format")
    @APIResponse(description = "OpenAPI specification retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/yaml",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.OpenApiYamlRouteKey.class)
    VertxRoute openApiYamlRoute(RouteBuilder.Factory factory, OpenApiHandler openApiHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(openApiHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.SWAGGER_UI_ROUTE)
    @Operation(summary = "Get Swagger UI",
               description = "Returns the Swagger UI for interactive API documentation")
    @APIResponse(description = "Swagger UI page retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "text/html",
                 schema = @Schema(type = SchemaType.STRING)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SwaggerUIRouteKey.class)
    VertxRoute swaggerUIRoute(RouteBuilder.Factory factory, SwaggerUIHandler swaggerUIHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(swaggerUIHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.WEBJARS_ROUTE)
    @Operation(summary = "Get WebJar resources",
               description = "Serves WebJar resources for Swagger UI")
    @APIResponse(description = "WebJar resource retrieved successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/javascript",
                 schema = @Schema(type = SchemaType.STRING)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.WebJarRouteKey.class)
    VertxRoute webJarRoute(RouteBuilder.Factory factory, WebJarHandler webJarHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(webJarHandler)
                      .build();
    }
}
