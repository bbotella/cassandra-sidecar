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

package org.apache.cassandra.sidecar.docs;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that the /api/v1/docs endpoint serves the Swagger UI correctly
 */
@ExtendWith(VertxExtension.class)
class SwaggerUIEndpointTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SwaggerUIEndpointTest.class);
    
    private Server server;
    private Vertx vertx;
    private WebClient webClient;
    private int port;

    @BeforeEach
    void setUp(VertxTestContext testContext) throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(SidecarModules.all()).with(new TestModule()));
        server = injector.getInstance(Server.class);
        vertx = injector.getInstance(Vertx.class);
        
        server.start()
              .onSuccess(s -> {
                  port = server.actualPort();
                  webClient = WebClient.create(vertx, new WebClientOptions()
                      .setDefaultHost("localhost")
                      .setDefaultPort(port));
                  testContext.completeNow();
              })
              .onFailure(testContext::failNow);
              
        assertTrue(testContext.awaitCompletion(30, TimeUnit.SECONDS), "Server should start within 30 seconds");
    }

    @AfterEach
    void tearDown(VertxTestContext testContext)
    {
        if (webClient != null)
        {
            webClient.close();
        }
        
        if (server != null)
        {
            server.stop("Test completed")
                  .onComplete(result -> testContext.completeNow());
        }
        else
        {
            testContext.completeNow();
        }
    }

    @Test
    void testSwaggerUIEndpoint(VertxTestContext testContext)
    {
        webClient.get("/api/v1/docs")
                 .send(testContext.succeeding(response -> testContext.verify(() -> {
                     assertEquals(200, response.statusCode());
                     assertEquals("text/html; charset=utf-8", response.getHeader("Content-Type"));
                     
                     String body = response.bodyAsString();
                     assertThat(body).isNotEmpty();
                     assertThat(body).contains("<!DOCTYPE html>");
                     assertThat(body).contains("Cassandra Sidecar API Documentation");
                     assertThat(body).contains("swagger-ui");
                     assertThat(body).contains("/api/v1/openapi.json");
                     
                     LOGGER.info("Swagger UI endpoint test passed");
                     testContext.completeNow();
                 })));
    }

    @Test
    void testOpenAPIJsonEndpoint(VertxTestContext testContext)
    {
        webClient.get("/api/v1/openapi.json")
                 .send(testContext.succeeding(response -> testContext.verify(() -> {
                     assertEquals(200, response.statusCode());
                     assertEquals("application/json", response.getHeader("Content-Type"));
                     
                     String body = response.bodyAsString();
                     assertThat(body).isNotEmpty();
                     assertThat(body).contains("\"openapi\"");
                     assertThat(body).contains("\"info\"");
                     assertThat(body).contains("\"paths\"");
                     assertThat(body).contains("Cassandra Sidecar API");
                     
                     LOGGER.info("OpenAPI JSON endpoint test passed");
                     testContext.completeNow();
                 })));
    }

    @Test
    void testSwaggerUIWithRawHttpClient(VertxTestContext testContext)
    {
        HttpClient client = vertx.createHttpClient(new HttpClientOptions()
            .setDefaultHost("localhost")
            .setDefaultPort(port));
        
        client.request(HttpMethod.GET, "/api/v1/docs")
              .compose(HttpClientRequest::send)
              .onSuccess(response -> {
                  testContext.verify(() -> {
                      assertEquals(200, response.statusCode());
                      assertEquals("text/html; charset=utf-8", response.getHeader("Content-Type"));
                      
                      LOGGER.info("Raw HTTP client test - Status: {}, Content-Type: {}", 
                                 response.statusCode(), response.getHeader("Content-Type"));
                  });
                  
                  response.body().onSuccess(buffer -> testContext.verify(() -> {
                      String body = buffer.toString();
                      assertThat(body).isNotEmpty();
                      assertThat(body).contains("<!DOCTYPE html>");
                      assertThat(body).contains("Cassandra Sidecar API Documentation");
                      
                      LOGGER.info("Raw HTTP client test - Body length: {}", body.length());
                      testContext.completeNow();
                  })).onFailure(testContext::failNow);
              })
              .onFailure(throwable -> {
                  LOGGER.error("Failed to make request to /api/v1/docs", throwable);
                  testContext.failNow(throwable);
              });
    }

    @Test
    void testEndpointReturnsValidHTML(VertxTestContext testContext)
    {
        webClient.get("/api/v1/docs")
                 .send(testContext.succeeding(response -> testContext.verify(() -> {
                     assertEquals(200, response.statusCode());
                     
                     String body = response.bodyAsString();
                     assertThat(body).isNotEmpty();
                     
                     // Check for essential HTML structure
                     assertThat(body).contains("<html");
                     assertThat(body).contains("<head>");
                     assertThat(body).contains("<body>");
                     assertThat(body).contains("</html>");
                     
                     // Check for Swagger UI specific elements with CDN URLs
                     assertThat(body).contains("https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js");
                     assertThat(body).contains("https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-standalone-preset.js");
                     assertThat(body).contains("SwaggerUIBundle");
                     
                     // Check that it references the OpenAPI JSON endpoint
                     assertThat(body).contains("/api/v1/openapi.json");
                     
                     LOGGER.info("HTML validation test passed - content length: {}", body.length());
                     testContext.completeNow();
                 })));
    }

    @Test
    void testWebJarEndpointNotNeeded(VertxTestContext testContext)
    {
        // Since we switched to CDN URLs, WebJar endpoints are no longer needed
        // but we test to document the current state
        webClient.get("/webjars/swagger-ui/5.17.14/swagger-ui-bundle.js")
                 .send(testContext.succeeding(response -> testContext.verify(() -> {
                     LOGGER.info("WebJar endpoint - Status: {}, Content-Type: {}", 
                                response.statusCode(), response.getHeader("Content-Type"));
                     
                     // Since we switched to CDN, this should return 404
                     // This test documents that WebJar resources are no longer served
                     assertEquals(404, response.statusCode(), 
                                "WebJar endpoint should return 404 since we switched to CDN URLs");
                     
                     LOGGER.info("WebJar endpoint correctly returns 404 - CDN URLs are used instead");
                     testContext.completeNow();
                 })));
    }

    @Test
    void testWebJarCssEndpointNotNeeded(VertxTestContext testContext)
    {
        // Since we switched to CDN URLs, WebJar CSS endpoints are no longer needed
        // but we test to document the current state
        webClient.get("/webjars/swagger-ui/5.17.14/swagger-ui-bundle.css")
                 .send(testContext.succeeding(response -> testContext.verify(() -> {
                     LOGGER.info("WebJar CSS endpoint - Status: {}, Content-Type: {}", 
                                response.statusCode(), response.getHeader("Content-Type"));
                     
                     // Since we switched to CDN, this should return 404
                     // This test documents that WebJar CSS resources are no longer served
                     assertEquals(404, response.statusCode(), 
                                "WebJar CSS endpoint should return 404 since we switched to CDN URLs");
                     
                     LOGGER.info("WebJar CSS endpoint correctly returns 404 - CDN URLs are used instead");
                     testContext.completeNow();
                 })));
    }
}
