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

import java.util.Collections;
import java.util.Set;

import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.TableStatsResponse;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.Parameter;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Handler for retrieving table stats
 */
public class TableStatsHandler extends AbstractHandler<QualifiedTableName> implements AccessProtected
{

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected TableStatsHandler(InstanceMetadataFetcher metadataFetcher,
                                ExecutorPools executorPools,
                                CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.STATS.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Operation(summary = "Get Table Statistics",
               description = "Retrieves statistics for a specific table in a keyspace, including SSTable count, disk space usage, and snapshot sizes.")
    @Parameter(name = "keyspace",
               in = ParameterIn.PATH,
               description = "The name of the keyspace.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "table",
               in = ParameterIn.PATH,
               description = "The name of the table.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @APIResponse(responseCode = "200",
                 description = "Successfully retrieved table statistics.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = TableStatsResponse.class)))
    @APIResponse(responseCode = "404",
                 description = "The specified keyspace or table was not found.")
    @APIResponse(responseCode = "500",
                 description = "Failed to retrieve table statistics.")
    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, String host, SocketAddress remoteAddress, QualifiedTableName tableName)
    {
        MetricsOperations operations = metadataFetcher.delegate(host).metricsOperations();
        executorPools.service()
                     .executeBlocking(() -> operations.tableStats(tableName))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, tableName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected QualifiedTableName extractParamsOrThrow(RoutingContext context)
    {
        return qualifiedTableName(context);
    }
}
