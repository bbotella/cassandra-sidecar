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

package org.apache.cassandra.sidecar.handlers.sstableuploads;

import java.nio.file.NoSuchFileException;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions;
import org.apache.cassandra.sidecar.common.response.SSTableImportResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.handlers.data.SSTableImportRequestParam;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.apache.cassandra.sidecar.utils.SSTableUploadsPathBuilder;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.Parameter;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE_SCOPE;
// SSTableImportResponse is already imported
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Imports SSTables, that have been previously uploaded, into Cassandra
 */
public class SSTableImportHandler extends AbstractHandler<SSTableImportRequestParam> implements AccessProtected
{
    private final SSTableImporter importer;
    private final SSTableUploadsPathBuilder uploadPathBuilder;
    private final Cache<SSTableImporter.ImportOptions, Future<Void>> cache;

    /**
     * Constructs a handler with the provided {@code metadataFetcher} and {@code builder} for the SSTableUploads
     * staging directory
     *
     * @param metadataFetcher   a class for fetching InstanceMetadata
     * @param importer          a class that handles importing the requests into Cassandra
     * @param uploadPathBuilder a class that provides SSTableUploads directories
     * @param cacheFactory      a factory for caches used in sidecar
     * @param executorPools     executor pools for blocking executions
     * @param validator         a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected SSTableImportHandler(InstanceMetadataFetcher metadataFetcher,
                                   SSTableImporter importer,
                                   SSTableUploadsPathBuilder uploadPathBuilder,
                                   CacheFactory cacheFactory,
                                   ExecutorPools executorPools,
                                   CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
        this.importer = importer;
        this.uploadPathBuilder = uploadPathBuilder;
        this.cache = cacheFactory.ssTableImportCache();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        Set<String> eligibleResources = TABLE_SCOPE.expandedResources();
        Authorization modifyAuthorization = CassandraPermissions.MODIFY.toAuthorization(eligibleResources);
        Authorization importAuthorization = BasicPermissions.IMPORT_STAGED_SSTABLE.toAuthorization();
        return Set.of(modifyAuthorization, importAuthorization);
    }

    /**
     * Import SSTables, that have been previously uploaded, into the Cassandra service
     *
     * @param context the context for the handler
     */
    @Override
    @Operation(summary = "Import Uploaded SSTables",
               description = "Imports previously uploaded SSTable components for a given upload session into the Cassandra table. Various import options can be specified as query parameters.")
    @Parameter(name = "uploadId",
               in = ParameterIn.PATH,
               description = "Identifier for the upload session containing the SSTables to import.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "keyspace",
               in = ParameterIn.PATH,
               description = "Target keyspace for the SSTable import.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "table",
               in = ParameterIn.PATH,
               description = "Target table for the SSTable import.",
               required = true,
               schema = @Schema(type = SchemaType.STRING))
    @Parameter(name = "resetLevel",
               in = ParameterIn.QUERY,
               description = "Reset level on new SSTables. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "clearRepaired",
               in = ParameterIn.QUERY,
               description = "Clear repaired info from new SSTables. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "verifySSTables",
               in = ParameterIn.QUERY,
               description = "Verify that new SSTables are not corrupt. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "verifyTokens",
               in = ParameterIn.QUERY,
               description = "Verify that tokens in new SSTables are owned by the current node. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "invalidateCaches",
               in = ParameterIn.QUERY,
               description = "Invalidate row cache for keys in new SSTables. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "extendedVerify",
               in = ParameterIn.QUERY,
               description = "Run extended verify checking all values in new SSTables. Default: true.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "true"))
    @Parameter(name = "copyData",
               in = ParameterIn.QUERY,
               description = "Copy data from source paths instead of moving them. Default: false.",
               required = false,
               schema = @Schema(type = SchemaType.BOOLEAN, defaultValue = "false"))
    @APIResponse(responseCode = "200",
                 description = "SSTable import completed successfully.",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = SSTableImportResponse.class)))
    @APIResponse(responseCode = "202",
                 description = "SSTable import has been accepted and is in progress.")
    @APIResponse(responseCode = "400",
                 description = "Bad request (e.g., invalid options).")
    @APIResponse(responseCode = "404",
                 description = "Not found (e.g., upload directory or specified table/keyspace does not exist).")
    @APIResponse(responseCode = "500",
                 description = "Internal server error during import process.")
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               SSTableImportRequestParam request)
    {
        uploadPathBuilder.build(host, request)
                         .onSuccess(uploadDirectory -> {
                             SSTableImporter.ImportOptions importOptions =
                             importOptions(host, request, uploadDirectory);

                             Future<Void> importResult = cache.get(importOptions, this::importSSTablesAsync);
                             if (importResult == null)
                             {
                                 // cache is disabled
                                 importResult = importSSTablesAsync(importOptions);
                             }

                             if (!importResult.isComplete())
                             {
                                 logger.debug("ImportHandler accepted request={}, remoteAddress={}, instance={}",
                                              request, remoteAddress, host);
                                 context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code()).end();
                             }
                             else if (importResult.failed())
                             {
                                 processFailure(importResult.cause(), context, host, remoteAddress, request);
                             }
                             else
                             {
                                 context.json(new SSTableImportResponse(true,
                                                                        request.uploadId(),
                                                                        request.keyspace().name(),
                                                                        request.table().name()));
                                 logger.debug("ImportHandler completed request={}, remoteAddress={}, instance={}",
                                              request, remoteAddress, host);
                             }
                         })
                         .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  SSTableImportRequestParam request)
    {
        if (cause instanceof NoSuchFileException)
        {
            logger.error("Upload directory not found for request={}, remoteAddress={}, " +
                         "instance={}", request, remoteAddress, host, cause);
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
        }
        else if (cause instanceof IllegalArgumentException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(),
                                           cause));
        }
        else if (cause instanceof HttpException)
        {
            context.fail(cause);
        }

        super.processFailure(cause, context, host, remoteAddress, request);
    }

    @Override
    protected SSTableImportRequestParam extractParamsOrThrow(RoutingContext context)
    {
        return SSTableImportRequestParam.from(qualifiedTableName(context, true), context);
    }

    /**
     * Schedules the SSTable import when the Cassandra service is available.
     *
     * @param importOptions the import options
     * @return a future for the import
     */
    private Future<Void> importSSTablesAsync(SSTableImporter.ImportOptions importOptions)
    {
        try
        {
            // ensure that table operations are available from the delegate before doing the import
            // otherwise fail fast propagating the HttpException
            metadataFetcher.delegate(importOptions.host()).tableOperations();
            return uploadPathBuilder.isValidDirectory(importOptions.directory())
                                    .compose(validDirectory -> importer.scheduleImport(importOptions));
        }
        catch (CassandraUnavailableException exception)
        {
            return Future.failedFuture(exception);
        }
    }

    private static SSTableImporter.ImportOptions importOptions(String host, SSTableImportRequestParam request,
                                                               String uploadDirectory)
    {
        return new SSTableImporter.ImportOptions.Builder()
               .host(host)
               .keyspace(request.keyspace().name())
               .tableName(request.table().name())
               .directory(uploadDirectory)
               .uploadId(request.uploadId())
               .resetLevel(request.resetLevel())
               .clearRepaired(request.clearRepaired())
               .verifySSTables(request.verifySSTables())
               .verifyTokens(request.verifyTokens())
               .invalidateCaches(request.invalidateCaches())
               .extendedVerify(request.extendedVerify())
               .copyData(request.copyData())
               .build();
    }
}
