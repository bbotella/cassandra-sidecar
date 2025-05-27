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

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.apache.cassandra.sidecar.job.NodeDecommissionJob;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.OperationalJobUtils;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.Parameter;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.RequestUtils.parseBooleanQueryParam;
// OperationalJobResponse is already imported

/**
 * Provides REST API for asynchronously decommissioning the corresponding Cassandra node
 */
public class NodeDecommissionHandler extends AbstractHandler<Boolean> implements AccessProtected
{
    private final OperationalJobManager jobManager;
    private final ServiceConfiguration config;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected NodeDecommissionHandler(InstanceMetadataFetcher metadataFetcher,
                                      ExecutorPools executorPools,
                                      ServiceConfiguration serviceConfiguration,
                                      CassandraInputValidator validator,
                                      OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
        this.config = serviceConfiguration;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.DECOMMISSION_NODE.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Operation(summary = "Decommission Cassandra Node",
               description = "Asynchronously decommissions the Cassandra node associated with this Sidecar instance. The 'force' parameter can be used to bypass safety checks, but should be used with extreme caution.")
    @Parameter(name = "force",
               in = ParameterIn.QUERY,
               description = "Whether to force decommissioning, bypassing safety checks. Defaults to false. Use with caution.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN))
    @APIResponse(responseCode = "200",
                 description = "Decommission job has completed (succeeded or failed).",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = OperationalJobResponse.class)))
    @APIResponse(responseCode = "202",
                 description = "Decommission job has been accepted and is in progress.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = OperationalJobResponse.class)))
    @APIResponse(responseCode = "409",
                 description = "A conflicting operational job is already in progress.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = OperationalJobResponse.class)))
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               Boolean isForce)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        NodeDecommissionJob job = new NodeDecommissionJob(UUIDs.timeBased(), operations, isForce);
        try
        {
            jobManager.trySubmitJob(job);
        }
        catch (OperationalJobConflictException oje)
        {
            String reason = oje.getMessage();
            logger.error("Conflicting job encountered. reason={}", reason);
            context.response().setStatusCode(HttpResponseStatus.CONFLICT.code());
            context.json(new OperationalJobResponse(job.jobId(), OperationalJobStatus.FAILED, job.name(), reason));
            return;
        }

        // Get the result, waiting for the specified wait time for result
        job.asyncResult(executorPools.service(), config.operationalJobExecutionMaxWaitTime())
           .onComplete(v -> OperationalJobUtils.sendStatusBasedResponse(context, job));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Boolean extractParamsOrThrow(RoutingContext context)
    {
        return parseBooleanQueryParam(context.request(), "force", false);
    }
}
