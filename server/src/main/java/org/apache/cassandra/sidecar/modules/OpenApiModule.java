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
import org.apache.cassandra.sidecar.handlers.OpenApiHandler;
import org.apache.cassandra.sidecar.handlers.SwaggerUIHandler;
import org.apache.cassandra.sidecar.handlers.WebJarHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Module for OpenAPI documentation
 */
public class OpenApiModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.OpenApiRouteKey.class)
    VertxRoute openApiRoute(RouteBuilder.Factory factory, OpenApiHandler openApiHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(openApiHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SwaggerUIRouteKey.class)
    VertxRoute swaggerUIRoute(RouteBuilder.Factory factory, SwaggerUIHandler swaggerUIHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(swaggerUIHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.WebJarRouteKey.class)
    VertxRoute webJarRoute(RouteBuilder.Factory factory, WebJarHandler webJarHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(webJarHandler)
                      .build();
    }
}
