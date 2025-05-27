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
import java.util.List;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.restore.RestoreJobConsistencyChecker;
import org.apache.cassandra.sidecar.restore.RestoreJobProgress;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
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
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides a REST API for querying the progress of a {@link RestoreJob}
 * The response content can vary based on the {@link RestoreJobProgressFetchPolicy}
 */
@Singleton
public class RestoreJobProgressHandler extends AbstractHandler<RestoreJobProgressFetchPolicy> implements AccessProtected
{
    private final RestoreJobConsistencyChecker consistencyLevelChecker;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    public RestoreJobProgressHandler(InstanceMetadataFetcher metadataFetcher,
                                     ExecutorPools executorPools,
                                     CassandraInputValidator validator,
                                     RestoreJobConsistencyChecker consistencyLevelChecker)
    {
        super(metadataFetcher, executorPools, validator);
        this.consistencyLevelChecker = consistencyLevelChecker;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.READ_RESTORE_JOB.toAuthorization());
    }

    @Override
    protected RestoreJobProgressFetchPolicy extractParamsOrThrow(RoutingContext context)
    {
        List<String> fetchPolicyValues = context.queryParam(ApiEndpointsV1.FETCH_POLICY_QUERY_PARAM);
        if (fetchPolicyValues.isEmpty())
        {
            logger.info("No RestoreJobProgressFetchPolicy is specified, FIRST_FAILED policy is assumed");
            return RestoreJobProgressFetchPolicy.FIRST_FAILED;
        }
        else if (fetchPolicyValues.size() > 1)
        {
            logger.warn("Multiple RestoreJobProgressFetchPolicy are specified. Pick the first one.");
        }
        return RestoreJobProgressFetchPolicy.fromString(fetchPolicyValues.get(0));
    }

    @Override
    @Operation(summary = "Get Restore Job Progress",
               description = "Retrieves the progress of a specific restore job. The level of detail in the response depends on the 'fetch-policy' query parameter.")
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
    @Parameter(name = "fetch-policy",
               in = ParameterIn.QUERY,
               description = "Policy to determine which restore ranges to include in the response. 'FIRST_FAILED' (default): includes only the first failed range encountered. 'ALL_FAILED_AND_PENDING': includes all ranges that are either failed or pending. 'ALL': includes all ranges regardless of status.",
               required = false,
               schema = @Schema(type = SchemaType.STRING, enumeration = {"FIRST_FAILED", "ALL_FAILED_AND_PENDING", "ALL"}, defaultValue = "FIRST_FAILED"))
    @APIResponse(responseCode = "200",
                 description = "Successfully retrieved restore job progress.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = RestoreJobProgressResponsePayload.class)))
    @APIResponse(responseCode = "400",
                 description = "Bad request (e.g., invalid fetch policy, job not sidecar-managed, or sliceCount not set).")
    @APIResponse(responseCode = "404",
                 description = "Restore job not found (or table/keyspace not found by previous handlers).")
    @APIResponse(responseCode = "500",
                 description = "Internal server error retrieving job progress.")
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  RestoreJobProgressFetchPolicy fetchPolicy)
    {
        RoutingContextUtils
        .getAsFuture(context, SC_RESTORE_JOB)
        .map(this::validateSidecarManagedRestoreJob)
        .compose(restoreJob -> consistencyLevelChecker.check(restoreJob, fetchPolicy))
        .map(RestoreJobProgress::toResponsePayload)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, fetchPolicy));
    }

    private RestoreJob validateSidecarManagedRestoreJob(RestoreJob restoreJob)
    {
        if (!restoreJob.isManagedBySidecar())
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Only Sidecar-managed restore jobs are allowed. " +
                                    "jobId=" + restoreJob.jobId +
                                    " jobManager=" + restoreJob.restoreJobManager.name());
        }

        if (restoreJob.sliceCount == null)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    "Controller must set the sliceCount for Sidecar-managed restore job. " +
                                    "jobId=" + restoreJob.jobId);
        }
        return restoreJob;
    }
}
