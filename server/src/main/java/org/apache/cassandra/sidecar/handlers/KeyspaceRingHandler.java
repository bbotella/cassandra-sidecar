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

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.data.RingEntry;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
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
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * A handler that provides ring information for a specific keyspace for the Cassandra cluster
 */
@Singleton
public class KeyspaceRingHandler extends AbstractHandler<Name> implements AccessProtected
{
    @Inject
    public KeyspaceRingHandler(InstanceMetadataFetcher metadataFetcher,
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
        return Collections.singleton(BasicPermissions.READ_RING_KEYSPACE_SCOPED.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Operation(summary = "Get Cassandra Ring Information",
               description = "Retrieves the ring view for the Cassandra cluster. If the 'keyspace' path parameter is provided, it shows information specific to that keyspace; otherwise, it may show a more general view (behavior can depend on Cassandra version and configuration). The response is a list of ring entries, each detailing a node in the ring.")
    @Parameter(name = "keyspace",
               in = ParameterIn.PATH,
               description = "The name of the keyspace. If not provided (i.e., when accessing /api/v1/cassandra/ring), ring information potentially spanning all keyspaces or a default view is returned.",
               required = false,
               schema = @Schema(type = SchemaType.STRING))
    @APIResponse(responseCode = "200",
                 description = "Successfully retrieved ring information.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(type = SchemaType.ARRAY, implementation = RingEntry.class)))
    @APIResponse(responseCode = "404",
                 description = "The specified keyspace was not found (only applicable when a keyspace is provided).")
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Name keyspace)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        executorPools.service()
                     .executeBlocking(() -> operations.ring(keyspace))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, keyspace));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Name keyspace)
    {
        if (cause instanceof IllegalArgumentException &&
            StringUtils.contains(cause.getMessage(), ", does not exist"))
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage(), cause));
            return;
        }

        super.processFailure(cause, context, host, remoteAddress, keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return keyspace(context, true);
    }
}
