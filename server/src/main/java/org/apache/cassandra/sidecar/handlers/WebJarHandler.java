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
import io.swagger.v3.oas.annotations.Hidden;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;

/**
 * Handler that serves WebJar static assets
 */
@Hidden
@Singleton
public class WebJarHandler implements Handler<RoutingContext>
{
    private final StaticHandler staticHandler;

    @Inject
    public WebJarHandler()
    {
        this.staticHandler = StaticHandler.create("META-INF/resources")
                                          .setWebRoot("META-INF/resources")
                                          .setCachingEnabled(true)
                                          .setMaxAgeSeconds(86400); // Cache for 1 day
    }

    @Override
    public void handle(RoutingContext context)
    {
        staticHandler.handle(context);
    }
}