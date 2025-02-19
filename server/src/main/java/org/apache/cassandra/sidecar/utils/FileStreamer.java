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

package org.apache.cassandra.sidecar.utils;

import com.google.common.util.concurrent.SidecarRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.exceptions.RangeException;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.ThrottleConfiguration;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.StreamSSTableMetrics;
import org.apache.cassandra.sidecar.models.HttpResponse;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;
import static org.apache.cassandra.sidecar.utils.MetricUtils.parseSSTableComponent;

/**
 * General handler for serving files
 */
@Singleton
public class FileStreamer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStreamer.class);

    private final ExecutorPools executorPools;
    private final ThrottleConfiguration config;
    private final SidecarRateLimiter rateLimiter;
    private final InstanceMetadataFetcher instanceMetadataFetcher;

    @Inject
    public FileStreamer(ExecutorPools executorPools,
                        ServiceConfiguration config,
                        @Named("StreamRequestRateLimiter") SidecarRateLimiter rateLimiter,
                        InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.executorPools = executorPools;
        this.config = config.throttleConfiguration();
        this.rateLimiter = rateLimiter;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    /**
     * Streams the {@code filename file} with length {@code fileLength} for the (optionally) requested
     * {@code rangeHeader} using the provided {@code response}.
     *
     * @param response    the response to use
     * @param instanceId  Cassandra instance from which file is streamed
     * @param filename    the path to the file to serve
     * @param fileLength  the size of the file to serve
     * @param rangeHeader (optional) a string representing the requested range for the file
     * @return a future with the result of the streaming
     */
    public Future<Void> stream(HttpResponse response, int instanceId, String filename,
                               long fileLength, String rangeHeader)
    {
        return parseRangeHeader(rangeHeader, fileLength)
               .compose(range -> stream(response, instanceId, filename, fileLength, range));
    }

    /**
     * Streams the {@code filename file} with length {@code fileLength} for the requested
     * {@code range} using the provided {@code response}.
     *
     * @param response   the response to use
     * @param instanceId  Cassandra instance from which file is streamed
     * @param filename   the path to the file to serve
     * @param fileLength the size of the file to serve
     * @param range      the range to stream
     * @return a future with the result of the streaming
     */
    public Future<Void> stream(HttpResponse response, int instanceId, String filename, long fileLength, HttpRange range)
    {
        Promise<Void> promise = Promise.promise();
        try
        {
            acquireAndSend(response, instanceId, filename, fileLength, range, System.nanoTime(), promise);
        }
        catch (Throwable t)
        {
            promise.tryFail(t);
        }
        return promise.future();
    }

    /**
     * Send the file if rate-limiting is disabled or when it successfully acquires a permit from the
     * {@link SidecarRateLimiter}.
     *
     * @param response          the response to use
     * @param instanceId        Cassandra instance from which file is streamed
     * @param filename          the path to the file to serve
     * @param fileLength        the size of the file to serve
     * @param range             the range to stream
     * @param startTimeNanos    the start time of this request
     * @param promise a promise for the stream
     */
    private void acquireAndSend(HttpResponse response,
                                int instanceId,
                                String filename,
                                long fileLength,
                                HttpRange range,
                                long startTimeNanos,
                                Promise<Void> promise)
    {
        InstanceMetrics instanceMetrics = instanceMetadataFetcher.instance(instanceId).metrics();
        StreamSSTableMetrics streamSSTableMetrics = instanceMetrics.streamSSTable();
        if (acquire(response, instanceId, filename, fileLength, range, startTimeNanos, streamSSTableMetrics, promise))
        {
            // Stream data if rate limiting is disabled or if we acquire
            LOGGER.debug("Streaming range {} for file {} to client {}. Instance: {}", range, filename,
                         response.remoteAddress(), response.host());
            response.sendFile(filename, fileLength, range)
                    .onSuccess(v ->
                               {
                                   String component = parseSSTableComponent(filename);
                                   LOGGER.debug("Streamed file {} successfully to client {}. Instance: {}", filename,
                                                response.remoteAddress(), response.host());
                                   streamSSTableMetrics.forComponent(component).bytesStreamedRate.metric.mark(range.length());
                                   instanceMetrics.streamSSTable().totalBytesStreamedRate.metric.mark(range.length());
                                   promise.complete();
                               })
                    .onFailure(promise::fail);
        }
    }

    /**
     * Acquires a permit from the {@link SidecarRateLimiter} if it can be acquired immediately without
     * delay. Otherwise, it will retry acquiring the permit later in the future until it exhausts the
     * retry timeout, in which case it will ask the client to retry later in the future.
     *
     * @param response              the response to use
     * @param instanceId            Cassandra instance from which file is streamed
     * @param filename              the path to the file to serve
     * @param fileLength            the size of the file to serve
     * @param range                 the range to stream
     * @param startTimeNanos        the start time of this request
     * @param streamSSTableMetrics  metrics captured during streaming of SSTables
     * @param promise               a promise for the stream
     * @return {@code true} if the permit was acquired, {@code false} otherwise
     */
    private boolean acquire(HttpResponse response, int instanceId, String filename, long fileLength, HttpRange range,
                            long startTimeNanos, StreamSSTableMetrics streamSSTableMetrics, Promise<Void> promise)
    {
        if (rateLimiter.tryAcquire())
            return true;

        long waitTimeNanos = MICROSECONDS.toNanos(rateLimiter.queryWaitTimeInMicros());
        if (isTimeoutExceeded(startTimeNanos, waitTimeNanos))
        {
            LOGGER.warn("Retries for acquiring permit exhausted for client {}. Instance: {}. " +
                        "Asking client to retry after {} nanoseconds.", response.remoteAddress(), response.host(),
                        waitTimeNanos);
            response.setRetryAfterHeader(waitTimeNanos);
            streamSSTableMetrics.throttled.metric.update(1);
            promise.fail(new HttpException(TOO_MANY_REQUESTS.code(), "Retry exhausted"));
        }
        else
        {
            LOGGER.debug("Retrying streaming after {} nanos for client {}. Instance: {}", waitTimeNanos,
                         response.remoteAddress(), response.host());
            executorPools.service()
                         // Note: adding an extra millisecond is required for 2 reasons
                         // 1. setTimer does not like scheduling with 0 delay; it throws
                         // 2. the retry should be scheduled later than the waitTimeNanos, in order to ensure it can acquire
                         .setTimer(NANOSECONDS.toMillis(waitTimeNanos) + 1,
                                   t -> acquireAndSend(response, instanceId, filename, fileLength, range,
                                                       startTimeNanos, promise));
        }
        return false;
    }

    /**
     * @param startTimeNanos the request start time in nanoseconds
     * @param waitTimeNanos the estimated time to wait for the next available permit in nanoseconds
     *
     * @return true if we exceeded timeout, false otherwise
     * Note: for this check we take wait time for the request into consideration
     */
    private boolean isTimeoutExceeded(long startTimeNanos, long waitTimeNanos)
    {
        long nowNanos = System.nanoTime();
        long timeoutNanos = startTimeNanos + waitTimeNanos + config.timeout().to(NANOSECONDS);
        return timeoutNanos < nowNanos;
    }

    /**
     * Returns the requested range for the request, or the entire range if {@code rangeHeader} is null
     *
     * @param rangeHeader The range header from the request
     * @param fileLength  The length of the file
     * @return a succeeded future when the parsing is successful, a failed future when the range parsing fails
     */
    public Future<HttpRange> parseRangeHeader(String rangeHeader, long fileLength)
    {
        HttpRange fr = HttpRange.of(0, fileLength - 1);
        if (rangeHeader == null)
            return Future.succeededFuture(fr);

        try
        {
            // sidecar does not support multiple ranges as of now
            HttpRange hr = HttpRange.parseHeader(rangeHeader, fileLength);
            HttpRange intersect = fr.intersect(hr);
            LOGGER.debug("Calculated range {} for streaming", intersect);
            return Future.succeededFuture(intersect);
        }
        catch (IllegalArgumentException | RangeException | UnsupportedOperationException e)
        {
            LOGGER.error("Failed to parse header '{}'", rangeHeader, e);
            return Future.failedFuture(wrapHttpException(REQUESTED_RANGE_NOT_SATISFIABLE, e.getMessage(), e));
        }
    }
}
