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
import org.apache.cassandra.sidecar.handlers.StreamSSTableComponentHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.ClearSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.CreateSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.snapshots.ListSnapshotHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableCleanupHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableImportHandler;
import org.apache.cassandra.sidecar.handlers.sstableuploads.SSTableUploadHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Provides the capability to access SSTables in the companion Cassandra node(s).
 * <ul>
 *     <li>Read capability: routes to take snapshots, list and download sstables from snapshots, remove snapshots</li>
 *     <li>Write capability: upload and import SSTables</li>
 * </ul>
 */
public class SSTablesAccessModule extends AbstractModule
{
    @Tag(name = "Streaming", description = "File streaming operations")
    @Operation(
        summary = "Stream SSTable component",
        description = "Streams SSTable component files from the Cassandra node"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "SSTable component streamed successfully",
            content = @Content(
                mediaType = "application/octet-stream"
            )
        ),
        @ApiResponse(
            responseCode = "404",
            description = "SSTable component not found"
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamSSTableComponentsRouteKey.class)
    VertxRoute streamSSTableComponentsRoute(RouteBuilder.Factory factory,
                                            StreamSSTableComponentHandler streamSSTableComponentHandler,
                                            FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedStreamSSTableComponentsRouteKey.class)
    VertxRoute deprecatedStreamSSTableComponentsRoute(RouteBuilder.Factory factory,
                                                      StreamSSTableComponentHandler streamSSTableComponentHandler,
                                                      FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.StreamSSTableComponentsWithSecondaryIndexRouteKey.class)
    VertxRoute streamSSTableComponentsWithSecondaryIndexRoute(RouteBuilder.Factory factory,
                                                              StreamSSTableComponentHandler streamSSTableComponentHandler,
                                                              FileStreamHandler fileStreamHandler)
    {
        return factory.builderForRoute()
                      .handler(streamSSTableComponentHandler)
                      .handler(fileStreamHandler)
                      .build();
    }

    @Tag(name = "Snapshots", description = "Snapshot management operations")
    @Operation(
        summary = "Create snapshot",
        description = "Creates a snapshot for the specified keyspace and table"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Snapshot created successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"result\": \"Success\", \"snapshotName\": \"backup_20240101\"," +
                                          " \"keyspace\": \"test_keyspace\", \"table\": \"test_table\"}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CreateSnapshotRouteKey.class)
    VertxRoute createSnapshotRouteKey(RouteBuilder.Factory factory,
                                      CreateSnapshotHandler createSnapshotHandler)
    {
        return factory.buildRouteWithHandler(createSnapshotHandler);
    }

    @Tag(name = "Snapshots", description = "Snapshot management operations")
    @Operation(
        summary = "List snapshots",
        description = "Lists snapshot files for the specified keyspace and table"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Snapshot files listed successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"snapshotFilesInfo\": [{\"size\": 1048576, \"host\": \"127.0.0.1\"," +
                                          " \"port\": 9042, \"dataDirIndex\": 0, \"snapshotName\": \"backup_20240101\"," +
                                          " \"keySpaceName\": \"test_keyspace\", \"tableName\": \"test_table\"," +
                                          " \"fileName\": \"mc-1-big-Data.db\"}]}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListSnapshotRouteKey.class)
    VertxRoute listSnapshotRouteKey(RouteBuilder.Factory factory,
                                    ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedListSnapshotRouteKey.class)
    VertxRoute deprecatedListSnapshotRouteKey(RouteBuilder.Factory factory,
                                              ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @Tag(name = "Snapshots", description = "Snapshot management operations")
    @Operation(
        summary = "Clear snapshot",
        description = "Clears/removes the specified snapshot"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Snapshot cleared successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"result\": \"Success\", \"message\": \"Snapshot cleared successfully\"}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ClearSnapshotRouteKey.class)
    VertxRoute clearSnapshotRouteKey(RouteBuilder.Factory factory,
                                     ValidateTableExistenceHandler validateTableExistence,
                                     ClearSnapshotHandler clearSnapshotHandler)
    {
        return factory.builderForRoute()
                      // Leverage the validateTableExistence. Currently, JMX does not validate for non-existent keyspace.
                      // Additionally, the current JMX implementation to clear snapshots does not support passing a table
                      // as a parameter.
                      .handler(validateTableExistence)
                      .handler(clearSnapshotHandler)
                      .build();
    }

    @Tag(name = "SSTable Operations", description = "Operations for managing SSTable uploads and imports")
    @Operation(
        summary = "Upload SSTable",
        description = "Uploads SSTable files to the Cassandra node for staging"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "SSTable uploaded successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"uploadId\": \"upload-789012\", \"uploadSizeBytes\": 2097152, \"serviceTimeMillis\": 1500}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableUploadRouteKey.class)
    VertxRoute sstableUploadRoute(RouteBuilder.Factory factory,
                                  SSTableUploadHandler sstableUploadHandler)
    {
        return factory.buildRouteWithHandler(sstableUploadHandler);
    }

    @Tag(name = "SSTable Operations", description = "Operations for managing SSTable uploads and imports")
    @Operation(
        summary = "Import SSTable",
        description = "Imports uploaded SSTable files into Cassandra"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "SSTable imported successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"success\": true, \"uploadId\": \"upload-123456\"," +
                                          " \"keyspace\": \"test_keyspace\", \"tableName\": \"test_table\"}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableImportRouteKey.class)
    VertxRoute sstableImportRoute(RouteBuilder.Factory factory,
                                  SSTableImportHandler sstableImportHandler)
    {
        return factory.buildRouteWithHandler(sstableImportHandler);
    }

    @Tag(name = "SSTable Operations", description = "Operations for managing SSTable uploads and imports")
    @Operation(
        summary = "Cleanup SSTables",
        description = "Cleans up uploaded SSTable staging files"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "SSTable cleanup completed successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(example = "{\"result\": \"Success\", \"cleanedFiles\": 5, \"freedSpace\": \"10MB\"}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableCleanupRouteKey.class)
    VertxRoute sstableCleanupRoute(RouteBuilder.Factory factory,
                                   SSTableCleanupHandler sstableCleanupHandler)
    {
        return factory.buildRouteWithHandler(sstableCleanupHandler);
    }
}
