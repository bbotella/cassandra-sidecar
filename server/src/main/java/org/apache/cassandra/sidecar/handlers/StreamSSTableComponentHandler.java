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


import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Set;
import javax.management.InstanceNotFoundException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.data.StreamSSTableComponentRequestParam;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
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

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE_SCOPE;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * This handler validates that the component exists in the cluster and sets up the context
 * for the {@link FileStreamHandler} to stream the component back to the client
 */
@Singleton
public class StreamSSTableComponentHandler extends AbstractHandler<StreamSSTableComponentRequestParam> implements AccessProtected
{
    private final SnapshotPathBuilder snapshotPathBuilder;

    @Inject
    public StreamSSTableComponentHandler(InstanceMetadataFetcher metadataFetcher,
                                         SnapshotPathBuilder snapshotPathBuilder,
                                         CassandraInputValidator validator,
                                         ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
        this.snapshotPathBuilder = snapshotPathBuilder;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        Set<String> eligibleResources = TABLE_SCOPE.expandedResources();
        Authorization stream = BasicPermissions.STREAM_SNAPSHOT.toAuthorization();
        Authorization select = CassandraPermissions.SELECT.toAuthorization(eligibleResources);
        return Set.of(stream, select);
    }

    @Override
    @Operation(summary = "Stream SSTable Component File",
               description = "Streams a specific component file of an SSTable from a snapshot. Supports specifying a data directory index and handles variations for secondary index components. Supports partial content streaming via the HTTP Range header.")
    @Parameter(name = "keyspace",
               in = ParameterIn.PATH,
               description = "Keyspace of the table.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "table",
               in = ParameterIn.PATH,
               description = "Table name (may include tableId suffix, e.g., 'mytable-aabbccdd').",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "snapshot",
               in = ParameterIn.PATH,
               description = "Name of the snapshot.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "component",
               in = ParameterIn.PATH,
               description = "Name of the SSTable component file (e.g., 'Data.db', 'Index.db').",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "index",
               in = ParameterIn.PATH,
               description = "Name of the secondary index (only for secondary index components).",
               required = false,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "dataDirectoryIndex",
               in = ParameterIn.QUERY,
               description = "Index of the Cassandra data directory where the component resides. Defaults to 0 if not specified.",
               required = false,
               schema = @Schema(type = SchemaType.INTEGER, defaultValue = "0"))
    @APIResponse(responseCode = "200",
                 description = "Successfully streamed the entire SSTable component.",
                 content = @Content(mediaType = "application/octet-stream",
                                    schema = @Schema(type = SchemaType.STRING, format = "binary")))
    @APIResponse(responseCode = "206",
                 description = "Successfully streamed a partial SSTable component based on the Range header.",
                 content = @Content(mediaType = "application/octet-stream",
                                    schema = @Schema(type = SchemaType.STRING, format = "binary")))
    @APIResponse(responseCode = "400",
                 description = "Bad request (e.g., invalid dataDirectoryIndex).")
    @APIResponse(responseCode = "404",
                 description = "Not found (e.g., keyspace, table, snapshot, or component does not exist).")
    @APIResponse(responseCode = "500",
                 description = "Internal server error during file resolution or streaming setup.")
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               StreamSSTableComponentRequestParam request)
    {
        resolveComponentPathFromRequest(host, request).onSuccess(path -> {
            logger.debug("{} resolved. path={}, request={}, remoteAddress={}, instance={}",
                         this.getClass().getSimpleName(), path, request, remoteAddress, host);
            context.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, path).next();
        }).onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    private Future<String> resolveComponentPathFromRequest(String host, StreamSSTableComponentRequestParam request)
    {
        return executorPools.internal().executeBlocking(() -> {
            int dataDirIndex = request.dataDirectoryIndex();
            if (request.tableId() != null)
            {
                StorageOperations storageOperations = metadataFetcher.delegate(host).storageOperations();
                List<String> dataDirList = storageOperations.dataFileLocations();
                if (dataDirIndex < 0 || dataDirIndex >= dataDirList.size())
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid data directory index: " + dataDirIndex);
                }
                return snapshotPathBuilder.resolveComponentPathFromDataDirectory(dataDirList.get(dataDirIndex), request);
            }
            else
            {
                logger.debug("Streaming SSTable component without a table Id. request={}, instance={}", request, host);
                TableOperations tableOperations = metadataFetcher.delegate(host).tableOperations();
                // asking jmx to give us the path for keyspace/table - tableId
                // as opposed to storageOperations.dataFileLocations, the table directory can change
                // when someone drops a table and recreates it with the same name, the table id will change
                // we do not keep a cache of the table directory data paths, so these requests always go
                // through JMX
                List<String> tableDirList = tableOperations.getDataPaths(request.keyspace(), request.tableName());
                if (dataDirIndex < 0 || dataDirIndex >= tableDirList.size())
                {
                    throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid data directory index: " + dataDirIndex);
                }

                return snapshotPathBuilder.resolveComponentPathFromTableDirectory(tableDirList.get(dataDirIndex), request);
            }
        });
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  StreamSSTableComponentRequestParam request)
    {
        String errMsg = "StreamSSTableComponentHandler failed. request={}, remoteAddress={}, instance={}";
        logger.error(errMsg, request, remoteAddress, host, cause);
        if (cause instanceof NoSuchFileException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else
        {
            InstanceNotFoundException instanceNotFoundException = ThrowableUtils.getCause(cause, InstanceNotFoundException.class);
            if (instanceNotFoundException != null)
            {
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, "keyspace/table combination not found"));
            }
            else
            {
                super.processFailure(cause, context, host, remoteAddress, request);
            }
        }
    }

    @Override
    protected StreamSSTableComponentRequestParam extractParamsOrThrow(RoutingContext context)
    {
        String tableNameParam = context.pathParam(TABLE_PATH_PARAM);
        Name tableName = validator.validateTableName(snapshotPathBuilder.maybeRemoveTableId(tableNameParam));

        QualifiedTableName qualifiedTableName = new QualifiedTableName(keyspace(context, true), tableName);
        return StreamSSTableComponentRequestParam.from(qualifiedTableName, context);
    }
}
