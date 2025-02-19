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

package org.apache.cassandra.sidecar;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Main class for initiating the Cassandra sidecar
 * Note: remember to start and stop all delegates of instances
 */
public class CassandraSidecarDaemon
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSidecarDaemon.class);
    @VisibleForTesting
    static Server runningApplication;

    public static void main(String[] args)
    {
        Path confPath = determineConfigPath();

        Server app = Guice.createInjector(new MainModule(confPath)).getInstance(Server.class);
        runningApplication = app;
        app.start().onSuccess(deploymentId -> Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (close(app))
            {
                LOGGER.info("Cassandra Sidecar stopped successfully");
            }
        }))).onFailure(throwable -> {
            LOGGER.error("Failed to start Sidecar", throwable);
            close(app);
            System.exit(1);
        });
    }

    /**
     * Closes the server, waits up to 1 minute for the server to shut down.
     *
     * @param app the server
     * @return {@code true} if the server shutdown successfully, {@code false} otherwise
     */
    @VisibleForTesting
    static boolean close(Server app)
    {
        try
        {
            app.close()
               .toCompletionStage()
               .toCompletableFuture()
               .get(1, TimeUnit.MINUTES);
            return true;
        }
        catch (Exception ex)
        {
            LOGGER.warn("Failed to stop Sidecar in 1 minute", ex);
        }
        return false;
    }

    private static Path determineConfigPath()
    {
        Path confPath;
        String yamlConfigurationPath = System.getProperty("sidecar.config");
        if (yamlConfigurationPath != null)
        {
            try
            {
                confPath = Paths.get(new URI(yamlConfigurationPath));
            }
            catch (Throwable e)
            {
                throw new IllegalArgumentException("Invalid URI: " + yamlConfigurationPath, e);
            }
        }
        else
        {
            confPath = Paths.get("conf/sidecar.yaml");
        }
        return ensurePathExists(confPath);
    }

    private static Path ensurePathExists(Path confPath)
    {
        if (!Files.exists(confPath))
        {
            throw new IllegalArgumentException(String.format("Sidecar configuration file '%s' does not exist",
                                                             confPath.toAbsolutePath()));
        }
        return confPath;
    }
}

