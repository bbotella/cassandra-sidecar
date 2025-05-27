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

package org.apache.cassandra.sidecar.handlers.restore;

import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
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

import static org.apache.cassandra.sidecar.routes.RoutingContextUtils.SC_RESTORE_JOB;
// RestoreJobSummaryResponsePayload is already imported
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for providing summary of restore job maintained by Sidecar
 */
@Singleton
public class RestoreJobSummaryHandler extends AbstractHandler<String> implements AccessProtected
{
    @Inject
    public RestoreJobSummaryHandler(ExecutorPools executorPools,
                                    InstanceMetadataFetcher instanceMetadataFetcher,
                                    CassandraInputValidator validator)
    {
        super(instanceMetadataFetcher, executorPools, validator);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.READ_RESTORE_JOB.toAuthorization());
    }

    @Override
    @Operation(summary = "Get Restore Job Summary",
               description = "Retrieves a summary of a specific restore job, including its creation time, agent, target keyspace/table, secrets (masked or partial), and current status.")
    @Parameter(name = "keyspace",
               in = ParameterIn.PATH,
               description = "Keyspace of the table for the restore job.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "table",
               in = ParameterIn.PATH,
               description = "Table name for the restore job.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "jobId",
               in = ParameterIn.PATH,
               description = "UUID of the restore job.",
               required = true,
               schema = @Schema(type = SchemaType.STRING, format = "uuid"))
    @APIResponse(responseCode = "200",
                 description = "Successfully retrieved restore job summary.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = RestoreJobSummaryResponsePayload.class)))
    @APIResponse(responseCode = "404",
                 description = "Restore job not found (or table/keyspace not found by previous handlers).")
    @APIResponse(responseCode = "500",
                 description = "Internal server error retrieving job summary.")
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  String jobId)
    {
        validateAndFindJob(context)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, jobId));
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.pathParam("jobId");
    }

    private Future<RestoreJobSummaryResponsePayload> validateAndFindJob(RoutingContext context)
    {
        return RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .compose(restoreJob -> {
            if (restoreJob.status == null || restoreJob.secrets == null)
            {
                logger.error("Restore job record read is missing required fields. job={}", restoreJob);
                return Future.failedFuture(wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                             "Restore job is missing required fields"));
            }
            RestoreJobSummaryResponsePayload response
            = new RestoreJobSummaryResponsePayload(restoreJob.createdAt.toString(), restoreJob.jobId,
                                                   restoreJob.jobAgent, restoreJob.keyspaceName, restoreJob.tableName,
                                                   restoreJob.secrets, restoreJob.statusWithOptionalDescription());
            return Future.succeededFuture(response);
        });
    }
}
