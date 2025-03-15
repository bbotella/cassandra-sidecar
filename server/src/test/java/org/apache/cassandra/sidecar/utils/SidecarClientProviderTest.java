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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.google.inject.Guice;
import com.google.inject.Injector;

import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestSslModule;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SidecarClientProviderTest
{

    Injector injector;
    private Vertx vertx;
    private Server server;

    SidecarClient client;
    TestModule testModule;

    @TempDir
    private Path certPath;

    private SidecarClientProvider provider;

    @BeforeEach
    void setup() throws Exception
    {
        testModule = new TestSslModule(certPath);

        injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(testModule));
        vertx = injector.getInstance(Vertx.class);
        server = getMTLSServerAndStart();
        provider = new SidecarClientProvider(vertx,
                                             injector.getInstance(SidecarConfiguration.class),
                                             new SidecarVersionProvider(),
                                             "localhost",
                                             server.actualPort());
        client = provider.get();
    }

    @AfterEach
    void cleanup()
    {
        if (server != null)
        {
            getBlocking(server.close(), 10, TimeUnit.SECONDS, "Close server");
        }
        getBlocking(vertx.close(), 10, TimeUnit.SECONDS, "Close vertx");
    }

    @Test
    void testSidecarClientIsSingleton()
    {
        SidecarClient client1 = provider.get();
        SidecarClient client2 = provider.get();

        assertThat(client1).isSameAs(client2);
    }

    @Test
    void testHotReloadOfClientCerts() throws Exception
    {
        Path expiredClientKeyStorePath = Path.of(ClassLoader.getSystemResource("certs/expired_server_keystore.p12").toURI());
        Path clientPath = certPath.resolve("certs/test.p12");
        Path clientBackupPath = certPath.resolve("certs/backup-test.p12");

        Files.copy(clientPath, clientBackupPath, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(expiredClientKeyStorePath, clientPath, StandardCopyOption.REPLACE_EXISTING);

        // Wait for the client to pick up the expired cert
        Thread.sleep(10000);
        unsuccessfulClientRequest(client);

        // Replace the expired certificated with a good certificate we can use
        Files.copy(clientBackupPath, clientPath, StandardCopyOption.REPLACE_EXISTING);

        // Wait until the client reloads the certificate
        for (int i = 0; i < 10; i++)
        {
            try
            {
                client.sidecarHealth()
                      .get(30, TimeUnit.SECONDS);
                break;
            }
            catch (Exception exception)
            {
                // Reload has not completed yet, we need ot wait until the client reloads the certificate
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }

        // Execute requests with the client. We should see successful requests go through now
        successfulClientRequest(client);
    }

    private void unsuccessfulClientRequest(SidecarClient client)
    {
        assertThatThrownBy(() -> client.sidecarHealth()
                                       .get(30, TimeUnit.SECONDS))
        .describedAs("Unsuccessful client requests are expected to fail")
        .isNotNull();
    }

    private void successfulClientRequest(SidecarClient client) throws Exception
    {
        HealthResponse healthResponse = client.sidecarHealth()
                                              .get(30, TimeUnit.SECONDS);
        assertThat(healthResponse).isNotNull();
        assertThat(healthResponse.isOk()).isTrue();
    }


    Server getMTLSServerAndStart() throws Exception
    {
        // Start server and wait for it to be running
        Server server = injector.getInstance(Server.class);
        server.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        return server;
    }
}
