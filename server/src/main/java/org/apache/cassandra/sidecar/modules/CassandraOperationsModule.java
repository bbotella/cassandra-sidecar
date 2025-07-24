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
import org.apache.cassandra.sidecar.handlers.ConnectedClientStatsHandler;
import org.apache.cassandra.sidecar.handlers.GossipInfoHandler;
import org.apache.cassandra.sidecar.handlers.GossipUpdateHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceRingHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceSchemaHandler;
import org.apache.cassandra.sidecar.handlers.ListOperationalJobsHandler;
import org.apache.cassandra.sidecar.handlers.NativeUpdateHandler;
import org.apache.cassandra.sidecar.handlers.NodeDecommissionHandler;
import org.apache.cassandra.sidecar.handlers.OperationalJobHandler;
import org.apache.cassandra.sidecar.handlers.RingHandler;
import org.apache.cassandra.sidecar.handlers.SchemaHandler;
import org.apache.cassandra.sidecar.handlers.StreamStatsHandler;
import org.apache.cassandra.sidecar.handlers.TableStatsHandler;
import org.apache.cassandra.sidecar.handlers.TokenRangeReplicaMapHandler;
import org.apache.cassandra.sidecar.handlers.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Provides the capability to query and invoke Cassandra operations
 */
public class CassandraOperationsModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraConnectedClientStatsRouteKey.class)
    VertxRoute cassandraConnectedClientStatsRoute(RouteBuilder.Factory factory,
                                                  ConnectedClientStatsHandler connectedClientStatsHandler)
    {
        return factory.buildRouteWithHandler(connectedClientStatsHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraOperationalJobRouteKey.class)
    VertxRoute cassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                            OperationalJobHandler operationalJobHandler)
    {
        return factory.buildRouteWithHandler(operationalJobHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListCassandraOperationalJobRouteKey.class)
    VertxRoute listCassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                                ListOperationalJobsHandler listOperationalJobsHandler)
    {
        return factory.buildRouteWithHandler(listOperationalJobsHandler);
    }

    @Tag(name = "Node Operations", description = "Node management operations")
    @Operation(
        summary = "Get node decommission status",
        description = "Returns the decommission status of a Cassandra node"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Node decommission status retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.OperationalJobResponse.class)
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeDecommissionRouteKey.class)
    VertxRoute cassandraNodeDecommissionRoute(RouteBuilder.Factory factory,
                                              NodeDecommissionHandler nodeDecommissionHandler)
    {
        return factory.buildRouteWithHandler(nodeDecommissionHandler);
    }

    @Tag(name = "Streaming", description = "File streaming operations")
    @Operation(
        summary = "Get stream statistics",
        description = "Returns streaming statistics for the Cassandra node"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Stream statistics retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.StreamStatsResponse.class)
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraStreamStatsRouteKey.class)
    VertxRoute cassandraStreamStatsRoute(RouteBuilder.Factory factory,
                                         StreamStatsHandler streamStatsHandler)
    {
        return factory.buildRouteWithHandler(streamStatsHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeSettingsRouteKey.class)
    VertxRoute cassandraNodeSettings(RouteBuilder.Factory factory,
                                     NodeSettingsHandler nodeSettingsHandler)
    {
        // Node settings endpoint is not Access protected. Any user who can log in into Cassandra is able to view
        // node settings information. Since sidecar and cassandra share list of authenticated identities, sidecar's
        // authenticated users can also read node settings information.
        return factory.builderForUnauthorizedRoute()
                      .handler(nodeSettingsHandler)
                      .build();
    }

    @Tag(name = "Schema", description = "Schema information endpoints")
    @Operation(
        summary = "Get all keyspaces schema",
        description = "Returns the schema information for all keyspaces in the Cassandra cluster"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Schema information retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.SchemaResponse.class, 
                    example = "{\"keyspace\": \"test_ks\", \"schema\": \"CREATE KEYSPACE test_ks...\"}")    
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.AllKeyspacesSchemaRouteKey.class)
    VertxRoute cassandraSchemaRoute(RouteBuilder.Factory factory,
                                    SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedAllKeyspacesSchemaRouteKey.class)
    VertxRoute deprecatedCassandraSchemaRoute(RouteBuilder.Factory factory,
                                              SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @Tag(name = "Schema", description = "Schema information endpoints")
    @Operation(
        summary = "Get keyspace schema",
        description = "Returns the schema information for a specific keyspace"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Keyspace schema retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.SchemaResponse.class, 
                    example = "{\"keyspace\": \"test_ks\", \"schema\": \"CREATE KEYSPACE test_ks...\"}")    
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.KeyspaceSchemaRouteKey.class)
    VertxRoute cassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                            KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedKeyspaceSchemaRouteKey.class)
    VertxRoute deprecatedCassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                                      KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @Tag(name = "Ring", description = "Cassandra cluster ring information")
    @Operation(
        summary = "Get cluster ring information",
        description = "Returns information about the Cassandra cluster ring topology"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Ring information retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.RingResponse.class, type = "array", 
                    example = "[{\"datacenter\": \"dc1\", \"address\": \"127.0.0.1\", \"port\": 7000, \"rack\": \"rack1\", \"status\": \"Up\", \"state\": \"Normal\", \"load\": \"1GB\", \"owns\": \"33%\", \"token\": \"12345\", \"fqdn\": \"node1\", \"hostId\": \"550e8400-e29b\"}]")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingRouteKey.class)
    VertxRoute cassandraRingRoute(RouteBuilder.Factory factory,
                                  RingHandler ringHandler)
    {
        return factory.buildRouteWithHandler(ringHandler);
    }

    @Tag(name = "Ring", description = "Cassandra cluster ring information")
    @Operation(
        summary = "Get keyspace ring information",
        description = "Returns ring information for a specific keyspace"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Keyspace ring information retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.RingResponse.class, type = "array", 
                    example = "[{\"datacenter\": \"dc1\", \"address\": \"127.0.0.1\", \"port\": 7000, \"rack\": \"rack1\", \"status\": \"Up\", \"state\": \"Normal\", \"load\": \"1GB\", \"owns\": \"33%\", \"token\": \"12345\", \"fqdn\": \"node1\", \"hostId\": \"550e8400-e29b\"}]")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingWithKeyspaceRouteKey.class)
    VertxRoute cassandraRingWithKeyspaceRoute(RouteBuilder.Factory factory,
                                              KeyspaceRingHandler keyspaceRingHandler)
    {
        return factory.buildRouteWithHandler(keyspaceRingHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraTokenRangeReplicaMapRouteKey.class)
    VertxRoute cassandraTokenRangeReplicaMapRoute(RouteBuilder.Factory factory,
                                                  TokenRangeReplicaMapHandler tokenRangeReplicaMapHandler)
    {
        return factory.buildRouteWithHandler(tokenRangeReplicaMapHandler);
    }

    @Tag(name = "Ring", description = "Cassandra cluster ring information")
    @Operation(
        summary = "Get gossip information",
        description = "Returns gossip information about the Cassandra cluster"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Gossip information retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = org.apache.cassandra.sidecar.common.response.GossipInfoResponse.class, 
                    example = "{\"/127.0.0.1:7000\": {\"generation\": \"1641024000\", \"heartbeat\": \"12345\", \"dc\": \"dc1\", \"rack\": \"rack1\", \"releaseVersion\": \"4.1.0\", \"schema\": \"uuid-12345\", \"load\": \"1GB\", \"hostId\": \"550e8400-e29b\"}}")
            )
        )
    })
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraGossipInfoRouteKey.class)
    VertxRoute cassandraGossipInfoRoute(RouteBuilder.Factory factory,
                                        GossipInfoHandler gossipInfoHandler)
    {
        return factory.buildRouteWithHandler(gossipInfoHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.TableStatsRouteKey.class)
    VertxRoute tableStatsRoute(RouteBuilder.Factory factory,
                               ValidateTableExistenceHandler validateTableExistenceHandler,
                               TableStatsHandler tableStatsHandler)
    {
        return factory.builderForRoute()
                      .handler(validateTableExistenceHandler)
                      .handler(tableStatsHandler).build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateNodeGossipStateRouteKey.class)
    VertxRoute cassandraChangeGossipStateRoute(RouteBuilder.Factory factory,
                                    GossipUpdateHandler nodeGossipHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(nodeGossipHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.UpdateNodeNativeStateRouteKey.class)
    VertxRoute cassandraChangeNativeStateRoute(RouteBuilder.Factory factory,
                                    NativeUpdateHandler nodeNativeHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(nodeNativeHandler)
                      .build();
    }
}
