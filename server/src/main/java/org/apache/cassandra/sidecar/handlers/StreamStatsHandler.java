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
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
// StreamStatsResponse is already imported
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.jetbrains.annotations.NotNull;

/**
 * Handler for retrieving node streams stats
 */
public class StreamStatsHandler extends AbstractHandler<Void> implements AccessProtected
{
    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the metadata fetcher
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    protected StreamStatsHandler(InstanceMetadataFetcher metadataFetcher,
                                 ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.STATS.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Operation(summary = "Get Node Stream Statistics",
               description = "Retrieves statistics about data streaming to and from the Cassandra instance, including the current operational mode and progress of streams.")
    @APIResponse(responseCode = "200",
                 description = "Successfully retrieved stream statistics.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = StreamStatsResponse.class)))
    @APIResponse(responseCode = "500", description = "Failed to retrieve stream statistics.")
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Void request)
    {

        CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);

        executorPools.service()
                     .executeBlocking(() -> {
                         String mode = delegate.storageOperations().operationMode();
                         StreamsProgressStats stats = delegate.metricsOperations().streamsProgressStats();
                         return new StreamStatsResponse(mode, stats);
                     })
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
