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
package org.apache.cassandra.sidecar.modules.multibindings;

import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;

/**
 * Class keys in the {@link com.google.inject.multibindings.MapBinder} to {@link org.apache.cassandra.sidecar.routes.VertxRoute} objects
 */
public interface VertxRouteMapKeys
{
    /** Handlers that are in the global scope, not binding to any specific route **/
    interface GlobalChainAuthHandlerKey extends ClassKey {}
    interface GlobalUtilityHandlerKey extends ClassKey {}
    interface GlobalErrorHandlerKey extends ClassKey {}
    /*-------*/

    /** Alphabetically sorted list of keys **/
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Abort restore job",
        description = "Aborts an active restore job and stops all associated operations",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore job aborted successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"ABORTED\", \"message\": \"Restore job aborted successfully\"}",
                           schemaRef = "#/components/schemas/AbortRestoreJobResponse")
        }
    )
    interface AbortRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.ABORT_RESTORE_JOB_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Schema",
        tagDescription = "Schema information endpoints",
        summary = "Get all keyspaces schema",
        description = "Returns the schema information for all keyspaces in the Cassandra cluster",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Schema information retrieved successfully", mediaType = "application/json", 
                           example = "{\"keyspace\": \"test_keyspace\", \"schema\": \"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.SchemaResponse.class)
        }
    )
    interface AllKeyspacesSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.ALL_KEYSPACES_SCHEMA_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Get connected client stats",
        description = "Returns statistics about connected clients to the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Connected client statistics retrieved successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface CassandraConnectedClientStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CONNECTED_CLIENT_STATS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Health",
        tagDescription = "Health check endpoints",
        summary = "Check Cassandra gossip health",
        description = "Returns the health status of Cassandra's gossip protocol",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Cassandra gossip is healthy", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object"),
            @OpenApiResponse(responseCode = "503", description = "Cassandra gossip is unhealthy", mediaType = "application/json", 
                           example = "{\"status\": \"UNHEALTHY\", \"message\": \"Gossip protocol is not responding\"}", schemaType = "object")
        }
    )
    interface CassandraGossipHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.GOSSIP_HEALTH_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Ring",
        tagDescription = "Cassandra cluster ring information",
        summary = "Get gossip information",
        description = "Returns gossip information about the Cassandra cluster",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Gossip information retrieved successfully", mediaType = "application/json", 
                           example = "{\"/127.0.0.1:7000\": {\"generation\": \"1641024000\", \"heartbeat\": \"12345\", \"dc\": \"dc1\", \"rack\": \"rack1\", \"releaseVersion\": \"4.1.0\", \"schema\": \"uuid-12345\", \"load\": \"1GB\", \"hostId\": \"550e8400-e29b\"}}",
                           schemaRef = "#/components/schemas/GossipInfoResponse")
        }
    )
    interface CassandraGossipInfoRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.GOSSIP_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Health",
        tagDescription = "Health check endpoints",
        summary = "Check Cassandra health",
        description = "Returns the overall health status of the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Cassandra is healthy", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object"),
            @OpenApiResponse(responseCode = "503", description = "Cassandra is unhealthy", mediaType = "application/json", 
                           example = "{\"status\": \"UNHEALTHY\", \"message\": \"Cassandra node is not responding\"}", schemaType = "object")
        }
    )
    interface CassandraHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_HEALTH_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Health",
        tagDescription = "Health check endpoints",
        summary = "Check Cassandra JMX health",
        description = "Returns the health status of Cassandra's JMX interface",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Cassandra JMX is healthy", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object"),
            @OpenApiResponse(responseCode = "503", description = "Cassandra JMX is unhealthy", mediaType = "application/json", 
                           example = "{\"status\": \"UNHEALTHY\", \"message\": \"JMX interface is not accessible\"}", schemaType = "object")
        }
    )
    interface CassandraJmxHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_JMX_HEALTH_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Health",
        tagDescription = "Health check endpoints",
        summary = "Check Cassandra native protocol health",
        description = "Returns the health status of Cassandra's native protocol interface",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Cassandra native protocol is healthy", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object"),
            @OpenApiResponse(responseCode = "503", description = "Cassandra native protocol is unhealthy", mediaType = "application/json", 
                           example = "{\"status\": \"UNHEALTHY\", \"message\": \"Native protocol is not responding\"}", schemaType = "object")
        }
    )
    interface CassandraNativeHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.CASSANDRA_NATIVE_HEALTH_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Get node decommission status",
        description = "Returns the decommission status of a Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Node decommission status retrieved successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"decommission-456\", \"operation\": \"DECOMMISSION\", \"status\": \"STARTED\", \"message\": \"Node decommission initiated\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.OperationalJobResponse.class)
        }
    )
    interface CassandraNodeDecommissionRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.NODE_DECOMMISSION_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Get node settings",
        description = "Returns configuration settings for the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Node settings retrieved successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface CassandraNodeSettingsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.NODE_SETTINGS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Get operational job status",
        description = "Returns the status of a specific operational job running on the node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Operational job status retrieved successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"decommission-456\", \"operation\": \"DECOMMISSION\", \"status\": \"STARTED\", \"message\": \"Node decommission initiated\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.OperationalJobResponse.class)
        }
    )
    interface CassandraOperationalJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPERATIONAL_JOB_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Ring",
        tagDescription = "Cassandra cluster ring information",
        summary = "Get cluster ring information",
        description = "Returns information about the Cassandra cluster ring topology",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Ring information retrieved successfully", mediaType = "application/json", 
                           example = "[{\"datacenter\": \"dc1\", \"address\": \"127.0.0.1\", \"port\": 7000, \"rack\": \"rack1\", \"status\": \"Up\", \"state\": \"Normal\", \"load\": \"1GB\", \"owns\": \"33%\", \"token\": \"12345\", \"fqdn\": \"node1\", \"hostId\": \"550e8400-e29b\"}]",
                           schemaRef = "#/components/schemas/RingResponse")
        }
    )
    interface CassandraRingRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RING_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Ring",
        tagDescription = "Cassandra cluster ring information",
        summary = "Get ring information for keyspace",
        description = "Returns ring information for a specific keyspace showing token ownership",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Ring information for keyspace retrieved successfully", mediaType = "application/json", 
                           example = "[{\"datacenter\": \"dc1\", \"address\": \"127.0.0.1\", \"port\": 7000, \"rack\": \"rack1\", \"status\": \"Up\", \"state\": \"Normal\", \"load\": \"1GB\", \"owns\": \"33%\", \"token\": \"12345\", \"fqdn\": \"node1\", \"hostId\": \"550e8400-e29b\"}]",
                           schemaRef = "#/components/schemas/RingResponse")
        }
    )
    interface CassandraRingWithKeyspaceRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RING_WITH_KEYSPACE_ROUTE;
    } // Tells the data ownership for the specific keyspace
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Get stream statistics",
        description = "Returns streaming statistics for the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Stream statistics retrieved successfully", mediaType = "application/json", 
                           example = "{\"operationMode\": \"JOINING\", \"streamsProgressStats\": {\"totalFilesToReceive\": 10, \"totalFilesReceived\": 7, \"totalBytesToReceive\": 104857600, \"totalBytesReceived\": 73400320, \"totalFilesToSend\": 5, \"totalFilesSent\": 3, \"totalBytesToSend\": 52428800, \"totalBytesSent\": 31457280}}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.StreamStatsResponse.class)
        }
    )
    interface CassandraStreamStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.STREAM_STATS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Ring",
        tagDescription = "Cassandra cluster ring information",
        summary = "Get token range replica mapping",
        description = "Returns the replica mapping for token ranges in a specific keyspace",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Token range replica mapping retrieved successfully", mediaType = "application/json", 
                           example = "{\"writeReplicas\": [\"127.0.0.1:9042\", \"127.0.0.2:9042\"], \"readReplicas\": [\"127.0.0.1:9042\", \"127.0.0.2:9042\", \"127.0.0.3:9042\"], \"tokenRange\": {\"start\": \"0\", \"end\": \"1000000000000000000\"}}",
                           schemaRef = "#/components/schemas/TokenRangeReplicasResponse")
        }
    )
    interface CassandraTokenRangeReplicaMapRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Snapshots",
        tagDescription = "Snapshot management operations",
        summary = "Clear snapshot",
        description = "Clears/deletes an existing snapshot",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Snapshot cleared successfully", mediaType = "application/json", 
                           example = "{\"result\": \"Success\", \"message\": \"Snapshot cleared successfully\"}",
                           schemaRef = "#/components/schemas/ClearSnapshotResponse")
        }
    )
    interface ClearSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Create restore job",
        description = "Creates a new restore job for importing data from backup sources",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore job created successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"CREATED\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.data.CreateRestoreJobResponsePayload.class)
        }
    )
    interface CreateRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.CREATE_RESTORE_JOB_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Create restore slice",
        description = "Creates a new restore slice as part of a restore job",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore slice created successfully", mediaType = "application/json", 
                           example = "{\"sliceId\": \"slice-789\", \"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"CREATED\"}",
                           schemaRef = "#/components/schemas/CreateRestoreSliceResponsePayload")
        }
    )
    interface CreateRestoreSliceRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.POST;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_SLICES_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Snapshots",
        tagDescription = "Snapshot management operations",
        summary = "Create snapshot",
        description = "Creates a snapshot for the specified keyspace and table",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Snapshot created successfully", mediaType = "application/json", 
                           example = "{\"result\": \"Success\", \"snapshotName\": \"backup_20240101\", \"keyspace\": \"test_keyspace\", \"table\": \"test_table\"}",
                           schemaRef = "#/components/schemas/CreateSnapshotResponse")
        }
    )
    interface CreateSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Schema",
        tagDescription = "Schema information endpoints",
        summary = "Report schema to DataHub",
        description = "Reports schema information to DataHub for data cataloging",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Schema reported successfully", mediaType = "application/json", 
                           example = "{\"result\": \"Success\", \"message\": \"Schema reported successfully\", \"timestamp\": \"2024-01-01T10:00:00Z\"}",
                           schemaRef = "#/components/schemas/ReportSchemaResponse")
        }
    )
    interface DataHubSchemaReportingRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.REPORT_SCHEMA_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Configuration",
        tagDescription = "Service configuration management",
        summary = "Delete service configuration",
        description = "Deletes a specific service configuration setting",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Service configuration deleted successfully", mediaType = "application/json", 
                           example = "{\"message\": \"Configuration deleted successfully\", \"deletedKey\": \"cdc.retention_hours\"}", schemaType = "object"),
            @OpenApiResponse(responseCode = "404", description = "Configuration key not found", mediaType = "application/json", 
                           example = "{\"error\": \"Configuration key not found\", \"code\": 404}", schemaType = "object")
        }
    )
    interface DeleteServiceConfigurationRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SERVICE_CONFIG_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Schema",
        tagDescription = "Schema information endpoints",
        summary = "Get all keyspaces schema (deprecated)",
        description = "Returns the schema information for all keyspaces in the Cassandra cluster. This endpoint is deprecated.",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Schema information retrieved successfully", mediaType = "application/json", 
                           example = "{\"keyspace\": \"test_keyspace\", \"schema\": \"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.SchemaResponse.class)
        }
    )
    interface DeprecatedAllKeyspacesSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_ALL_KEYSPACES_SCHEMA_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Schema",
        tagDescription = "Schema information endpoints",
        summary = "Get keyspace schema (deprecated)",
        description = "Returns the schema information for a specific keyspace. This endpoint is deprecated.",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Keyspace schema retrieved successfully", mediaType = "application/json", 
                           example = "{\"keyspace\": \"test_keyspace\", \"schema\": \"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.SchemaResponse.class)
        }
    )
    interface DeprecatedKeyspaceSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_KEYSPACE_SCHEMA_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Snapshots",
        tagDescription = "Snapshot management operations",
        summary = "List snapshots (deprecated)",
        description = "Lists all snapshots available on the node. This endpoint is deprecated.",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Snapshots listed successfully", mediaType = "application/json", 
                           example = "{\"snapshotFilesInfo\": [{\"size\": 1048576, \"host\": \"127.0.0.1\", \"port\": 9042, \"dataDirIndex\": 0, \"snapshotName\": \"backup_20240101\", \"keySpaceName\": \"test_keyspace\", \"tableName\": \"test_table\", \"fileName\": \"mc-1-big-Data.db\"}]}",
                           schemaRef = "#/components/schemas/ListSnapshotFilesResponse")
        }
    )
    interface DeprecatedListSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_SNAPSHOTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Stream SSTable components (deprecated)",
        description = "Streams SSTable component files from the Cassandra node. This endpoint is deprecated.",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable component stream initiated successfully", mediaType = "application/octet-stream", 
                           example = "[Binary SSTable component content]", schemaType = "string")
        }
    )
    interface DeprecatedStreamSSTableComponentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.DEPRECATED_COMPONENTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Configuration",
        tagDescription = "Service configuration management",
        summary = "Get all service configurations",
        description = "Returns all service configuration settings",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Service configurations retrieved successfully", mediaType = "application/json", 
                           example = "{\"configurations\": [{\"key\": \"cdc.retention_hours\", \"value\": \"24\"}, {\"key\": \"compaction.enabled\", \"value\": \"true\"}]}",
                           schemaClass = org.apache.cassandra.sidecar.common.request.data.AllServicesConfigPayload.class)
        }
    )
    interface GetAllServiceConfigurationsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SERVICES_CONFIG_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Get restore job progress",
        description = "Returns the progress information for a specific restore job",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore job progress retrieved successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"IN_PROGRESS\", \"progressPercentage\": 75.5, \"message\": \"Restoring data files...\", \"startTime\": \"2024-01-01T10:00:00Z\", \"elapsedTime\": \"PT45M30S\"}",
                           schemaRef = "#/components/schemas/RestoreJobProgressResponsePayload")
        }
    )
    interface GetRestoreJobProgressRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_PROGRESS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Get restore job summary",
        description = "Returns a summary of restore jobs",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore job summary retrieved successfully", mediaType = "application/json", 
                           example = "{\"jobs\": [{\"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"COMPLETED\", \"createdAt\": \"2024-01-01T10:00:00Z\", \"completedAt\": \"2024-01-01T11:30:00Z\"}]}",
                           schemaRef = "#/components/schemas/RestoreJobSummaryResponsePayload")
        }
    )
    interface GetRestoreJobSummaryRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Schema",
        tagDescription = "Schema information endpoints",
        summary = "Get keyspace schema",
        description = "Returns the schema information for a specific keyspace",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Keyspace schema retrieved successfully", mediaType = "application/json", 
                           example = "{\"keyspace\": \"test_keyspace\", \"schema\": \"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.SchemaResponse.class)
        }
    )
    interface KeyspaceSchemaRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "List operational jobs",
        description = "Returns a list of all operational jobs running on the node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Operational jobs listed successfully", mediaType = "application/json", 
                           example = "[{\"jobId\": \"decommission-456\", \"operation\": \"DECOMMISSION\", \"status\": \"STARTED\", \"message\": \"Node decommission initiated\"}]",
                           schemaType = "array")
        }
    )
    interface ListCassandraOperationalJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIST_OPERATIONAL_JOBS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "CDC",
        tagDescription = "Change Data Capture operations",
        summary = "List CDC segments",
        description = "Lists CDC (Change Data Capture) segments available for streaming",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "CDC segments listed successfully", mediaType = "application/json", 
                           example = "{\"segments\": [{\"filename\": \"CommitLog-7-1641024000000.log\", \"size\": 67108864, \"createdDate\": \"2024-01-01T10:00:00Z\"}]}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.ListCdcSegmentsResponse.class)
        }
    )
    interface ListCdcSegmentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIST_CDC_SEGMENTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Snapshots",
        tagDescription = "Snapshot management operations",
        summary = "List snapshot files",
        description = "Lists all files in existing snapshots",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Snapshot files listed successfully", mediaType = "application/json", 
                           example = "{\"snapshotFilesInfo\": [{\"size\": 1048576, \"host\": \"127.0.0.1\", \"port\": 9042, \"dataDirIndex\": 0, \"snapshotName\": \"backup_20240101\", \"keySpaceName\": \"test_keyspace\", \"tableName\": \"test_table\", \"fileName\": \"mc-1-big-Data.db\"}]}",
                           schemaRef = "#/components/schemas/ListSnapshotFilesResponse")
        }
    )
    interface ListSnapshotRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SNAPSHOTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "API Documentation",
        tagDescription = "API documentation and specification endpoints",
        summary = "Get OpenAPI specification",
        description = "Returns the OpenAPI specification for the Cassandra Sidecar API",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "OpenAPI specification retrieved successfully", mediaType = "application/json", 
                           example = "{\"openapi\": \"3.0.0\", \"info\": {\"title\": \"Cassandra Sidecar API\", \"version\": \"1.0\"}}", schemaType = "object")
        }
    )
    interface OpenApiRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.OPENAPI_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "API Documentation",
        tagDescription = "API documentation and specification endpoints",
        summary = "Get Swagger UI",
        description = "Returns the Swagger UI for interactive API documentation",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Swagger UI page retrieved successfully", mediaType = "text/html", 
                           example = "<!DOCTYPE html><html><head><title>Swagger UI</title></head><body><div id=\"swagger-ui\"></div></body></html>", schemaType = "string")
        }
    )
    interface SwaggerUIRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.SWAGGER_UI_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "API Documentation",
        tagDescription = "API documentation and specification endpoints",
        summary = "Get WebJar resources",
        description = "Serves WebJar resources for Swagger UI",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "WebJar resource retrieved successfully", mediaType = "application/javascript", 
                           example = "// Swagger UI JavaScript resource content", schemaType = "string")
        }
    )
    interface WebJarRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.WEBJARS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Live Migration",
        tagDescription = "Live migration operations for data transfer",
        summary = "Stream file for live migration",
        description = "Streams a file for live migration data transfer",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "File stream for live migration initiated successfully", mediaType = "application/octet-stream", 
                           example = "[Binary file content]", schemaType = "string"),
            @OpenApiResponse(responseCode = "403", description = "Live migration not enabled or file access denied", mediaType = "application/json", 
                           example = "{\"error\": \"Live migration not enabled or file access denied\", \"code\": 403}", schemaType = "object")
        }
    )
    interface LiveMigrationFileStreamHandlerRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILE_TRANSFER_API;
    }
    @OpenApiEndpoint(
        tag = "Live Migration",
        tagDescription = "Live migration operations for data transfer",
        summary = "List instance files",
        description = "Lists files available on an instance for live migration purposes",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Instance files listed successfully", mediaType = "application/json", 
                           example = "{\"files\": [{\"filename\": \"mc-1-big-Data.db\", \"size\": 1048576, \"lastModified\": \"2024-01-01T10:00:00Z\"}]}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse.class),
            @OpenApiResponse(responseCode = "403", description = "Live migration not enabled or node not configured for migration", mediaType = "application/json", 
                           example = "{\"error\": \"Live migration not enabled or node not configured for migration\", \"code\": 403}", schemaType = "object")
        }
    )
    interface LiveMigrationListInstanceFilesRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.LIVE_MIGRATION_FILES_API;
    }
    @OpenApiEndpoint(
        tag = "SSTable Operations",
        tagDescription = "Operations for managing SSTable uploads and imports",
        summary = "Clean up SSTable files",
        description = "Cleans up SSTable files to free up disk space",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable cleanup completed successfully", mediaType = "application/json", 
                           example = "{\"result\": \"Success\", \"cleanedFiles\": 5, \"freedSpace\": \"10MB\"}",
                           schemaRef = "#/components/schemas/SSTableCleanupResponse")
        }
    )
    interface SSTableCleanupRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.DELETE;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_CLEANUP_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "SSTable Operations",
        tagDescription = "Operations for managing SSTable uploads and imports",
        summary = "Import SSTable",
        description = "Imports previously uploaded SSTable files into the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable imported successfully", mediaType = "application/json", 
                           example = "{\"success\": true, \"uploadId\": \"upload-123456\", \"keyspace\": \"test_keyspace\", \"tableName\": \"test_table\"}",
                           schemaRef = "#/components/schemas/SSTableImportResponse")
        }
    )
    interface SSTableImportRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_IMPORT_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "SSTable Operations",
        tagDescription = "Operations for managing SSTable uploads and imports",
        summary = "Upload SSTable",
        description = "Uploads SSTable files to the Cassandra node for staging",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable uploaded successfully", mediaType = "application/json", 
                           example = "{\"uploadId\": \"upload-789012\", \"uploadSizeBytes\": 2097152, \"serviceTimeMillis\": 1500}",
                           schemaClass = org.apache.cassandra.sidecar.common.response.SSTableUploadResponse.class)
        }
    )
    interface SSTableUploadRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Health",
        tagDescription = "Health check endpoints",
        summary = "Check Sidecar health",
        description = "Returns the health status of the Sidecar application",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Sidecar is healthy", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface SidecarHealthRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.HEALTH_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "CDC",
        tagDescription = "Change Data Capture operations",
        summary = "Stream CDC segment",
        description = "Streams a specific CDC segment file for consumption",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "CDC segment stream initiated successfully", mediaType = "application/octet-stream", 
                           example = "[Binary CDC segment content]", schemaType = "string")
        }
    )
    interface StreamCdcSegmentRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.STREAM_CDC_SEGMENTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Stream SSTable components",
        description = "Streams SSTable component files from the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable component stream initiated successfully", mediaType = "application/octet-stream", 
                           example = "[Binary SSTable component content]", schemaType = "string")
        }
    )
    interface StreamSSTableComponentsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.COMPONENTS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Stream SSTable components with secondary index",
        description = "Streams SSTable component files with secondary index support",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "SSTable component with secondary index stream initiated successfully", mediaType = "application/octet-stream", 
                           example = "[Binary SSTable component with secondary index content]", schemaType = "string")
        }
    )
    interface StreamSSTableComponentsWithSecondaryIndexRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.COMPONENTS_WITH_SECONDARY_INDEX_ROUTE_SUPPORT;
    }
    @OpenApiEndpoint(
        tag = "Streaming",
        tagDescription = "File streaming operations",
        summary = "Get table statistics",
        description = "Returns statistics for a specific table",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Table statistics retrieved successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface TableStatsRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.TABLE_STATS_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Get time skew information",
        description = "Returns time skew information for the node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Time skew information retrieved successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface TimeSkewRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.GET;
        String ROUTE_URI = ApiEndpointsV1.TIME_SKEW_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Update node gossip state",
        description = "Updates the gossip state of the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Node gossip state updated successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface UpdateNodeGossipStateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String    ROUTE_URI   = ApiEndpointsV1.GOSSIP_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Node Operations",
        tagDescription = "Node management operations",
        summary = "Update node native state",
        description = "Updates the native protocol state of the Cassandra node",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Node native state updated successfully", mediaType = "application/json", 
                           example = "{\"status\": \"OK\"}",
                           schemaType = "object")
        }
    )
    interface UpdateNodeNativeStateRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String    ROUTE_URI   = ApiEndpointsV1.CASSANDRA_NATIVE_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Restore Jobs",
        tagDescription = "Restore job management operations",
        summary = "Update restore job",
        description = "Updates an existing restore job configuration",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Restore job updated successfully", mediaType = "application/json", 
                           example = "{\"jobId\": \"123e4567-e89b-12d3-a456-426614174000\", \"status\": \"UPDATED\", \"message\": \"Job configuration updated\"}",
                           schemaRef = "#/components/schemas/UpdateRestoreJobResponsePayload")
        }
    )
    interface UpdateRestoreJobRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PATCH;
        String ROUTE_URI = ApiEndpointsV1.RESTORE_JOB_ROUTE;
    }
    @OpenApiEndpoint(
        tag = "Configuration",
        tagDescription = "Service configuration management",
        summary = "Update service configuration",
        description = "Updates service configuration settings",
        responses = {
            @OpenApiResponse(responseCode = "200", description = "Service configuration updated successfully", mediaType = "application/json", 
                           example = "{\"message\": \"Service configuration updated successfully\", \"timestamp\": \"2024-01-01T10:00:00Z\"}", schemaType = "object")
        }
    )
    interface UpdateServiceConfigurationRouteKey extends RouteClassKey
    {
        HttpMethod HTTP_METHOD = HttpMethod.PUT;
        String ROUTE_URI = ApiEndpointsV1.SERVICE_CONFIG_ROUTE;
    }
}
