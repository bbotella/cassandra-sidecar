/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.coordination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.net.SSLOptions;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

/**
 * Periodically checks whether the key store file has changed. Triggers an update to the client's SSLOptions
 * whenever a file change has detected.
 */
public class ClientKeyStoreCheckPeriodicTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientKeyStoreCheckPeriodicTask.class);
    private final Vertx vertx;
    private final SslConfiguration sslConfiguration;
    private final HttpClient httpClient;
    private final SSLOptions sslOptions;
    private long lastModifiedTime = 0; // records the last modified timestamp

    public ClientKeyStoreCheckPeriodicTask(Vertx vertx,
                                           SslConfiguration sslConfiguration,
                                           HttpClient httpClient,
                                           SSLOptions sslOptions)
    {
        this.vertx = vertx;
        this.sslConfiguration = sslConfiguration;
        this.httpClient = httpClient;
        this.sslOptions = sslOptions;
        maybeRecordLastModifiedTime();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (sslConfiguration == null)
            return ScheduleDecision.SKIP;
        KeyStoreConfiguration keyStoreConfiguration = sslConfiguration.keystore();
        boolean shouldSkip = keyStoreConfiguration == null ||
                             !keyStoreConfiguration.isConfigured() ||
                             !keyStoreConfiguration.reloadStore();
        return shouldSkip ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
    }

    @Override
    public DurationSpec delay()
    {
        return  sslConfiguration == null || sslConfiguration.keystore() == null ? DEFAULT_DELAY : sslConfiguration.keystore().checkInterval();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        LOGGER.info("Running periodic client key store checker");
        String keyStorePath = sslConfiguration.keystore().path();
        vertx.fileSystem().props(keyStorePath)
             .onSuccess(props -> {
                 long previousLastModifiedTime = lastModifiedTime;
                 if (props.lastModifiedTime() != previousLastModifiedTime)
                 {
                     LOGGER.info("Client certificate file change detected for path={}, previousLastModifiedTime={}, " +
                                 "lastModifiedTime={}", keyStorePath, previousLastModifiedTime,
                                 props.lastModifiedTime());

                     // updates with the original ssl options, but it forces an SSL context reload
                     httpClient.updateSSLOptions(sslOptions, true)
                               .onSuccess(v -> {
                                   lastModifiedTime = props.lastModifiedTime();
                                   LOGGER.info("Completed reloading client certificates from path={}", keyStorePath);
                                   promise.complete(); // propagate successful completion
                               })
                               .onFailure(cause -> {
                                   LOGGER.error("Failed to reload client certificate from path={}", keyStorePath, cause);
                                   promise.fail(cause);
                               });
                 }
                 else
                 {
                     promise.complete(); // make sure to fulfill the promise
                 }
             })
             .onFailure(error -> {
                 LOGGER.warn("Unable to retrieve props for path={}", keyStorePath, error);
                 promise.fail(error);
             });
    }

    protected void maybeRecordLastModifiedTime()
    {
        if (scheduleDecision() == ScheduleDecision.SKIP)
        {
            return;
        }
        String keyStorePath = sslConfiguration.keystore().path();
        vertx.fileSystem().props(keyStorePath)
             .onSuccess(props -> lastModifiedTime = props.lastModifiedTime())
             .onFailure(err -> {
                 LOGGER.error("Unable to get lastModifiedTime for path={}", keyStorePath);
                 lastModifiedTime = -1;
             });
    }
}
