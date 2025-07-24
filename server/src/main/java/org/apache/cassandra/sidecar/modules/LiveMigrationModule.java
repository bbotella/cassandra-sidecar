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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationApiEnableDisableHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationFileStreamHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationListInstanceFilesHandler;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMapSidecarConfigImpl;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Module for supporting LiveMigration feature.
 */
public class LiveMigrationModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(LiveMigrationMap.class).to(LiveMigrationMapSidecarConfigImpl.class);
    }


    @Tag(name = "Live Migration", description = "Live migration operations for data transfer")
    @Operation(
        summary = "Download migration file",
        description = "Downloads a file as part of live migration data transfer between nodes"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "File downloaded successfully",
            content = @Content(
                mediaType = "application/octet-stream"
            )
        ),
        @ApiResponse(
            responseCode = "404",
            description = "File not found"
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Live migration not enabled or node not configured as source"
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationFileStreamHandlerRouteKey.class)
    VertxRoute downloadFileRoute(RouteBuilder.Factory factory,
                                 LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                 LiveMigrationFileStreamHandler liveMigrationFileStreamHandler,
                                 FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSource)
                      .handler(liveMigrationFileStreamHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @Tag(name = "Live Migration", description = "Live migration operations for data transfer")
    @Operation(
        summary = "List instance files",
        description = "Lists files available on an instance for live migration purposes"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Instance files listed successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"files\": [{\"filename\": \"mc-1-big-Data.db\", \"size\": 1048576, \"lastModified\": \"2024-01-01T10:00:00Z\"}]}")
            )
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Live migration not enabled or node not configured for migration"
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LiveMigrationListInstanceFilesRouteKey.class)
    VertxRoute listInstanceFiles(RouteBuilder.Factory factory,
                                 LiveMigrationApiEnableDisableHandler liveMigrationApiEnableDisableHandler,
                                 LiveMigrationListInstanceFilesHandler liveMigrationListInstanceFilesHandler)
    {
        return factory.builderForRoute()
                      .handler(liveMigrationApiEnableDisableHandler::isSourceOrDestination)
                      .handler(liveMigrationListInstanceFilesHandler)
                      .build();
    }
}
