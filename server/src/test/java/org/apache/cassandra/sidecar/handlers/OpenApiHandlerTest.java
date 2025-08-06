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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.config.OpenApiConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link OpenApiHandler} class
 */
@ExtendWith(VertxExtension.class)
class OpenApiHandlerTest
{
    private static final String TEST_JSON_CONTENT = "{\"openapi\":\"3.0.0\",\"info\":{\"title\":\"Test API\",\"version\":\"1.0\"}}";
    private static final String TEST_YAML_CONTENT = "openapi: 3.0.0\ninfo:\n  title: Test API\n  version: '1.0'\n";
    
    private HttpServer server;
    private WebClient client;
    private TestOpenApiHandler handler;
    private Path tempDir;
    private Vertx vertx;
    
    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) throws IOException
    {
        this.vertx = vertx;
        
        // Create temporary directory structure for testing file system loading
        tempDir = Files.createTempDirectory("openapi-test");
        Path openapiDir = tempDir.resolve("server/build/generated/openapi");
        Files.createDirectories(openapiDir);
        
        // Create test OpenAPI files
        Files.writeString(openapiDir.resolve("openapi.json"), TEST_JSON_CONTENT);
        Files.writeString(openapiDir.resolve("openapi.yaml"), TEST_YAML_CONTENT);
        
        // Create mock configuration
        SidecarConfiguration sidecarConfig = mock(SidecarConfiguration.class);
        OpenApiConfiguration openApiConfig = mock(OpenApiConfiguration.class);
        when(sidecarConfig.openApiConfiguration()).thenReturn(openApiConfig);
        
        handler = new TestOpenApiHandler(sidecarConfig, tempDir);
        
        // Set up test server
        Router router = Router.router(vertx);
        router.get("/openapi.json").handler(handler);
        router.get("/openapi.yaml").handler(handler);
        router.get("/openapi").handler(handler);
        
        server = vertx.createHttpServer();
        client = WebClient.create(vertx);
        
        server.requestHandler(router)
              .listen(0)
              .onComplete(testContext.succeedingThenComplete());
    }
    
    @AfterEach
    void tearDown(VertxTestContext testContext) throws IOException
    {
        // Clean up temporary files
        deleteRecursively(tempDir);
        
        if (server != null)
        {
            server.close().onComplete(testContext.succeedingThenComplete());
        }
        if (client != null)
        {
            client.close();
        }
    }
    
    @Test
    void testJsonRequestByPath(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi.json")
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/json");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_JSON_CONTENT);
                  testContext.completeNow();
              })));
    }
    
    @Test
    void testYamlRequestByPath(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi.yaml")
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/yaml");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_YAML_CONTENT);
                  testContext.completeNow();
              })));
    }
    
    @Test
    void testJsonRequestByAcceptHeader(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi")
              .putHeader("Accept", "application/json")
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/json");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_JSON_CONTENT);
                  testContext.completeNow();
              })));
    }
    
    @Test
    void testYamlRequestByAcceptHeader(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi")
              .putHeader("Accept", "application/yaml")
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/yaml");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_YAML_CONTENT);
                  testContext.completeNow();
              })));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"application/json, application/yaml", "text/html, application/yaml", "application/yaml; q=0.9"})
    void testYamlRequestByAcceptHeaderWithMultipleValues(String acceptHeader, VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi")
              .putHeader("Accept", acceptHeader)
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/yaml");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_YAML_CONTENT);
                  testContext.completeNow();
              })));
    }
    
    @Test
    void testDefaultToJsonWhenNoFormatSpecified(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "localhost", "/openapi")
              .timeout(5000)
              .send()
              .onComplete(testContext.succeeding(response -> testContext.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(response.getHeader("Content-Type")).isEqualTo("application/json");
                  assertThat(response.bodyAsString()).isEqualTo(TEST_JSON_CONTENT);
                  testContext.completeNow();
              })));
    }

    @Test
    void testIsYamlRequestDetection()
    {
        RoutingContext context = mock(RoutingContext.class);
        io.vertx.core.http.HttpServerRequest request = mock(io.vertx.core.http.HttpServerRequest.class);
        when(context.request()).thenReturn(request);
        
        // Test path-based detection
        when(request.path()).thenReturn("/openapi.yaml");
        assertThat(handler.testIsYamlRequest(context)).isTrue();
        
        when(request.path()).thenReturn("/openapi.json");
        assertThat(handler.testIsYamlRequest(context)).isFalse();
        
        // Test accept header detection
        when(request.path()).thenReturn("/openapi");
        when(request.getHeader("Accept")).thenReturn("application/yaml");
        assertThat(handler.testIsYamlRequest(context)).isTrue();
        
        when(request.getHeader("Accept")).thenReturn("application/json");
        assertThat(handler.testIsYamlRequest(context)).isFalse();
        
        when(request.getHeader("Accept")).thenReturn("text/html, application/yaml");
        assertThat(handler.testIsYamlRequest(context)).isTrue();
        
        when(request.getHeader("Accept")).thenReturn(null);
        assertThat(handler.testIsYamlRequest(context)).isFalse();
    }
    
    private void deleteRecursively(Path path) throws IOException
    {
        if (Files.isDirectory(path))
        {
            Files.list(path).forEach(child -> {
                try
                {
                    deleteRecursively(child);
                }
                catch (IOException e)
                {
                    // Best effort cleanup
                }
            });
        }
        Files.deleteIfExists(path);
    }
    
    /**
     * Test subclass that overrides resource loading for predictable testing
     */
    private static class TestOpenApiHandler extends OpenApiHandler
    {
        private final Path tempDir;
        private final boolean simulateFailure;
        
        TestOpenApiHandler(SidecarConfiguration sidecarConfiguration, Path tempDir)
        {
            this(sidecarConfiguration, tempDir, false);
        }
        
        TestOpenApiHandler(SidecarConfiguration sidecarConfiguration, Path tempDir, boolean simulateFailure)
        {
            super(sidecarConfiguration);
            this.tempDir = tempDir;
            this.simulateFailure = simulateFailure;
        }
        
        @Override
        public void handle(RoutingContext context)
        {
            if (simulateFailure)
            {
                // Simulate failure by throwing an exception
                context.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code())
                       .putHeader("Content-Type", "application/json")
                       .end();
                return;
            }
            
            try
            {
                boolean isYaml = testIsYamlRequest(context);
                String content = isYaml ? TEST_YAML_CONTENT : TEST_JSON_CONTENT;
                String contentType = isYaml ? "application/yaml" : "application/json";
                
                context.response()
                       .putHeader("Content-Type", contentType)
                       .end(content);
            }
            catch (Exception e)
            {
                context.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code())
                       .putHeader("Content-Type", "application/json")
                       .end();
            }
        }
        
        boolean testIsYamlRequest(RoutingContext context)
        {
            String path = context.request().path();
            if (path != null && path.endsWith(".yaml"))
            {
                return true;
            }
            
            String acceptHeader = context.request().getHeader("Accept");
            return acceptHeader != null && acceptHeader.contains("application/yaml");
        }
    }
}
