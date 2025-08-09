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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link OpenApiHandler} class
 */
@ExtendWith(VertxExtension.class)
class OpenApiHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenApiHandlerTest.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    
    private Vertx vertx;
    private Server server;

    @BeforeEach
    void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(new TestModule()));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        if (server != null)
        {
            CountDownLatch closeLatch = new CountDownLatch(1);
            server.close().onSuccess(res -> closeLatch.countDown());
            if (closeLatch.await(60, TimeUnit.SECONDS))
                LOGGER.info("Close event received before timeout.");
            else
                LOGGER.error("Close event timed out.");
        }
    }

    @Test
    void testJsonRequestByPath(VertxTestContext testContext)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/spec/openapi.json")
              .as(BodyCodec.buffer())
              .send(resp -> {
                  HttpResponse<Buffer> response = resp.result();
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/json");
                  
                  String body = response.bodyAsString();
                  assertThat(body).isNotEmpty();
                  
                  // Validate that the response is valid JSON
                  try
                  {
                      JSON_MAPPER.readTree(body);
                      testContext.completeNow();
                  }
                  catch (Exception e)
                  {
                      testContext.failNow(new AssertionError("Response is not valid JSON: " + e.getMessage()));
                  }
              });
    }

    @Test
    void testYamlRequestByPath(VertxTestContext testContext)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/spec/openapi.yaml")
              .as(BodyCodec.buffer())
              .send(resp -> {
                  HttpResponse<Buffer> response = resp.result();
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/yaml");
                  
                  String body = response.bodyAsString();
                  assertThat(body).isNotEmpty();
                  
                  // Validate that the response is valid YAML
                  try
                  {
                      YAML_MAPPER.readTree(body);
                      testContext.completeNow();
                  }
                  catch (Exception e)
                  {
                      testContext.failNow(new AssertionError("Response is not valid YAML: " + e.getMessage()));
                  }
              });
    }

    @Test
    void testYamlRequestByAcceptHeader(VertxTestContext testContext)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "localhost", "/spec/openapi")
              .putHeader("Accept", "application/yaml")
              .as(BodyCodec.buffer())
              .send(resp -> {
                  HttpResponse<Buffer> response = resp.result();
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/yaml");

                  String body = response.bodyAsString();
                  assertThat(body).isNotEmpty();

                  // Validate that the response is valid YAML
                  try
                  {
                      YAML_MAPPER.readTree(body);
                      testContext.completeNow();
                  }
                  catch (Exception e)
                  {
                      testContext.failNow(new AssertionError("Response is not valid YAML: " + e.getMessage()));
                  }
              });
    }
}
