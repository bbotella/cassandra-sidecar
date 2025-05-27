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

import com.google.inject.Inject;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.common.response.TimeSkewResponse;
import org.apache.cassandra.sidecar.utils.TimeSkewInfo;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/**
 * Provides clients information about the current time on this host
 * and the allowable time skew between this host and the client.
 */
@Path("/api/v1/time-skew")
public class TimeSkewHandler implements Handler<RoutingContext>
{
    private final TimeSkewInfo timeSkewInfo;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param timeSkewInfo the time skew info provider
     */
    @Inject
    protected TimeSkewHandler(TimeSkewInfo timeSkewInfo)
    {
        this.timeSkewInfo = timeSkewInfo;
    }

    @GET
    @Operation(summary = "Get Time Skew Information",
               description = "Provides the current server time and the allowable time skew between the server and the client.")
    @APIResponse(responseCode = "200",
                 description = "Current server time and allowable skew",
                 content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = TimeSkewResponse.class)))
    @Override
    public void handle(RoutingContext context)
    {
        context.json(timeSkewInfo.timeSkewResponse());
    }
}
