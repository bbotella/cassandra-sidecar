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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.SSTableUploadResponse;
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
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Provides the capability to access SSTables in the companion Cassandra node(s).
 * <ul>
 *     <li>Read capability: routes to take snapshots, list and download sstables from snapshots, remove snapshots</li>
 *     <li>Write capability: upload and import SSTables</li>
 * </ul>
 */
@Path("/")
public class SSTablesAccessModule extends AbstractModule
{
    @GET
    @Path(ApiEndpointsV1.COMPONENTS_ROUTE)
    @Operation(summary = "Stream SSTable components",
               description = "Streams SSTable component files from the Cassandra node")
    @APIResponse(description = "SSTable component stream initiated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/octet-stream",
                 schema = @Schema(type = SchemaType.STRING)))
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
    @GET
    @Path(ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE)
    @Operation(summary = "Stream SSTable components (deprecated)",
               description = "Streams SSTable component files from the Cassandra node. This endpoint is deprecated.")
    @APIResponse(description = "SSTable component stream initiated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/octet-stream",
                 schema = @Schema(type = SchemaType.STRING)))
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

    @GET
    @Path(ApiEndpointsV1.COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT)
    @Operation(summary = "Stream SSTable components with secondary index",
               description = "Streams SSTable component files with secondary index support")
    @APIResponse(description = "SSTable component with secondary index stream initiated successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/octet-stream",
                 schema = @Schema(type = SchemaType.STRING)))
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

    @PUT
    @Path(ApiEndpointsV1.SNAPSHOTS_ROUTE)
    @Operation(summary = "Create snapshot",
               description = "Creates a snapshot for the specified keyspace and table")
    @APIResponse(description = "Snapshot created successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CreateSnapshotRouteKey.class)
    VertxRoute createSnapshotRouteKey(RouteBuilder.Factory factory,
                                      CreateSnapshotHandler createSnapshotHandler)
    {
        return factory.buildRouteWithHandler(createSnapshotHandler);
    }

    @GET
    @Path(ApiEndpointsV1.SNAPSHOTS_ROUTE)
    @Operation(summary = "List snapshot files",
               description = "Lists all files in existing snapshots")
    @APIResponse(description = "Snapshot files listed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListSnapshotRouteKey.class)
    VertxRoute listSnapshotRouteKey(RouteBuilder.Factory factory,
                                    ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @Deprecated
    @GET
    @Path(ApiEndpointsV1.DEPRECATED_SNAPSHOTS_ROUTE)
    @Operation(summary = "List snapshots (deprecated)",
               description = "Lists all snapshots available on the node. This endpoint is deprecated.")
    @APIResponse(description = "Snapshots listed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedListSnapshotRouteKey.class)
    VertxRoute deprecatedListSnapshotRouteKey(RouteBuilder.Factory factory,
                                              ListSnapshotHandler listSnapshotHandler)
    {
        return factory.buildRouteWithHandler(listSnapshotHandler);
    }

    @DELETE
    @Path(ApiEndpointsV1.SNAPSHOTS_ROUTE)
    @Operation(summary = "Clear snapshot",
               description = "Clears/deletes an existing snapshot")
    @APIResponse(description = "Snapshot cleared successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
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

    @PUT
    @Path(ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE)
    @Operation(summary = "Upload SSTable",
               description = "Uploads SSTable files to the Cassandra node for staging")
    @APIResponse(description = "SSTable uploaded successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(implementation = SSTableUploadResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableUploadRouteKey.class)
    VertxRoute sstableUploadRoute(RouteBuilder.Factory factory,
                                  SSTableUploadHandler sstableUploadHandler)
    {
        return factory.buildRouteWithHandler(sstableUploadHandler);
    }

    @PUT
    @Path(ApiEndpointsV1.SSTABLE_IMPORT_ROUTE)
    @Operation(summary = "Import SSTable",
               description = "Imports previously uploaded SSTable files into the Cassandra node")
    @APIResponse(description = "SSTable imported successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableImportRouteKey.class)
    VertxRoute sstableImportRoute(RouteBuilder.Factory factory,
                                  SSTableImportHandler sstableImportHandler)
    {
        return factory.buildRouteWithHandler(sstableImportHandler);
    }

    @DELETE
    @Path(ApiEndpointsV1.SSTABLE_CLEANUP_ROUTE)
    @Operation(summary = "Clean up SSTable files",
               description = "Cleans up SSTable files to free up disk space")
    @APIResponse(description = "SSTable cleanup completed successfully",
                 responseCode = "200",
                 content = @Content(mediaType = "application/json",
                 schema = @Schema(type = SchemaType.OBJECT)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.SSTableCleanupRouteKey.class)
    VertxRoute sstableCleanupRoute(RouteBuilder.Factory factory,
                                   SSTableCleanupHandler sstableCleanupHandler)
    {
        return factory.buildRouteWithHandler(sstableCleanupHandler);
    }
}
