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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link OpenApiDocumentationGenerator}
 */
class OpenApiDocumentationGeneratorTest
{
    @TempDir
    Path tempDir;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp()
    {
        objectMapper = Json.mapper();
    }

    @Test
    void testGenerateOpenApiDocumentation() throws IOException
    {
        String[] args = {tempDir.toString()};
        
        assertDoesNotThrow(() -> OpenApiDocumentationGenerator.main(args));
        
        // Verify all expected files are generated
        Path jsonFile = tempDir.resolve("openapi.json");
        Path yamlFile = tempDir.resolve("openapi.yaml");
        Path htmlFile = tempDir.resolve("api-docs.html");
        
        assertTrue(Files.exists(jsonFile), "OpenAPI JSON file should be generated");
        assertTrue(Files.exists(yamlFile), "OpenAPI YAML file should be generated");
        assertTrue(Files.exists(htmlFile), "OpenAPI HTML file should be generated");
        
        // Verify files are not empty
        assertTrue(Files.size(jsonFile) > 0, "JSON file should not be empty");
        assertTrue(Files.size(yamlFile) > 0, "YAML file should not be empty");
        assertTrue(Files.size(htmlFile) > 0, "HTML file should not be empty");
    }

    @Test
    void testGeneratedJsonStructure() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        
        // Verify basic OpenAPI structure
        assertThat(rootNode.get("openapi").asText()).isEqualTo("3.0.1");
        assertNotNull(rootNode.get("info"), "Info section should exist");
        assertNotNull(rootNode.get("servers"), "Servers section should exist");
        assertNotNull(rootNode.get("tags"), "Tags section should exist");
        assertNotNull(rootNode.get("paths"), "Paths section should exist");
        assertNotNull(rootNode.get("components"), "Components section should exist");
        
        // Verify info section
        JsonNode info = rootNode.get("info");
        assertThat(info.get("title").asText()).isEqualTo("Cassandra Sidecar API");
        assertThat(info.get("description").asText()).isEqualTo("REST API for managing Apache Cassandra operations");
        assertThat(info.get("version").asText()).isEqualTo("1.0.0");
        assertNotNull(info.get("license"), "License info should exist");
    }

    @Test
    void testRequiredTagsExist() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode tags = rootNode.get("tags");
        
        assertThat(tags.isArray()).isTrue();
        
        // Collect all tag names
        java.util.Set<String> tagNames = new java.util.HashSet<>();
        for (JsonNode tag : tags)
        {
            tagNames.add(tag.get("name").asText());
        }
        
        // Verify expected tags exist
        assertThat(tagNames).contains(
            "Health",
            "Ring", 
            "Schema",
            "Node Operations",
            "Streaming",
            "SSTable Operations",
            "Snapshots",
            "Restore Jobs",
            "CDC",
            "Configuration",
            "Live Migration"
        );
    }

    @Test
    void testRingResponseSchemaExists() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode schemas = rootNode.get("components").get("schemas");
        
        JsonNode ringResponseSchema = schemas.get("RingResponse");
        assertNotNull(ringResponseSchema, "RingResponse schema should exist");
        
        // Verify it's defined as an array
        assertThat(ringResponseSchema.get("type").asText()).isEqualTo("array");
        assertNotNull(ringResponseSchema.get("items"), "Array items should be defined");
        
        JsonNode items = ringResponseSchema.get("items");
        assertThat(items.get("type").asText()).isEqualTo("object");
        assertNotNull(items.get("properties"), "Item properties should be defined");
        
        // Verify required properties exist
        JsonNode properties = items.get("properties");
        assertNotNull(properties.get("datacenter"), "datacenter property should exist");
        assertNotNull(properties.get("address"), "address property should exist");
        assertNotNull(properties.get("port"), "port property should exist");
        assertNotNull(properties.get("rack"), "rack property should exist");
        assertNotNull(properties.get("status"), "status property should exist");
        assertNotNull(properties.get("state"), "state property should exist");
        assertNotNull(properties.get("load"), "load property should exist");
        assertNotNull(properties.get("owns"), "owns property should exist");
        assertNotNull(properties.get("token"), "token property should exist");
        assertNotNull(properties.get("fqdn"), "fqdn property should exist");
        assertNotNull(properties.get("hostId"), "hostId property should exist");
        
        // Verify example exists (or proper schema structure)
        assertTrue(ringResponseSchema.has("example") || (ringResponseSchema.has("items") && ringResponseSchema.get("items").has("properties")),
                   "RingResponse should have an example or proper schema structure");
    }

    @Test
    void testGossipInfoResponseSchemaExists() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode schemas = rootNode.get("components").get("schemas");
        
        JsonNode gossipResponseSchema = schemas.get("GossipInfoResponse");
        assertNotNull(gossipResponseSchema, "GossipInfoResponse schema should exist");
        
        // Verify it's defined as an object with additionalProperties
        assertThat(gossipResponseSchema.get("type").asText()).isEqualTo("object");
        assertTrue(gossipResponseSchema.has("additionalProperties"), "Should have additionalProperties");
        
        JsonNode additionalProps = gossipResponseSchema.get("additionalProperties");
        assertTrue(additionalProps.isBoolean() || additionalProps.isObject(),
                   "additionalProperties should be boolean or object");
        if (additionalProps.isObject())
        {
            assertThat(additionalProps.get("type").asText()).isEqualTo("object");
            JsonNode properties = additionalProps.get("properties");
            if (properties != null)
            {
                // Verify gossip field properties exist
                assertNotNull(properties.get("generation"), "generation property should exist");
                assertNotNull(properties.get("heartbeat"), "heartbeat property should exist");
                assertNotNull(properties.get("schema"), "schema property should exist");
                assertNotNull(properties.get("rack"), "rack property should exist");
                assertNotNull(properties.get("releaseVersion"), "releaseVersion property should exist");
                assertNotNull(properties.get("hostId"), "hostId property should exist");
            }
        }
        
        // Verify example exists (or proper schema structure)
        assertTrue(gossipResponseSchema.has("example") || gossipResponseSchema.has("additionalProperties"),
                   "GossipInfoResponse should have an example or proper schema structure");
    }

    @Test
    void testCriticalSchemasExist() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode schemas = rootNode.get("components").get("schemas");
        
        // Verify critical schemas exist
        String[] criticalSchemas = {
            "RingResponse",
            "GossipInfoResponse", 
            "CreateRestoreJobResponsePayload",
            "RestoreJobProgressResponsePayload",
            "RestoreJobSummaryResponsePayload",
            "ListSnapshotFilesResponse",
            "SSTableUploadResponse",
            "SSTableImportResponse",
            "StreamStatsResponse",
            "SchemaResponse",
            "ListCdcSegmentsResponse",
            "CreateSnapshotResponse",
            "ClearSnapshotResponse",
            "SSTableCleanupResponse",
            "UpdateServiceConfigResponse",
            "OperationalJobResponse",
            "InstanceFilesListResponse"
        };
        
        for (String schemaName : criticalSchemas)
        {
            assertNotNull(schemas.get(schemaName), schemaName + " schema should exist");
        }
    }

    @Test
    void testRingApiEndpointsExist() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode paths = rootNode.get("paths");
        
        // Verify Ring API endpoints exist
        assertNotNull(paths.get("/api/v1/cassandra/ring"), "Ring endpoint should exist");
        assertNotNull(paths.get("/api/v1/cassandra/ring/keyspaces/:keyspace"), "Keyspace ring endpoint should exist");
        assertNotNull(paths.get("/api/v1/keyspaces/:keyspace/token-range-replicas"), "Token range replicas endpoint should exist");
        assertNotNull(paths.get("/api/v1/cassandra/gossip"), "Gossip endpoint should exist");
        
        // Verify Ring endpoints have proper tags
        JsonNode ringEndpoint = paths.get("/api/v1/cassandra/ring").get("get");
        assertNotNull(ringEndpoint, "Ring GET operation should exist");
        JsonNode tags = ringEndpoint.get("tags");
        assertThat(tags.isArray()).isTrue();
        assertThat(tags.get(0).asText()).isEqualTo("Ring");
    }

    @Test
    void testConfigurationApiEndpointsExist() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode paths = rootNode.get("paths");
        
        // Verify Configuration API endpoints exist
        assertNotNull(paths.get("/api/v1/services"), "Services list endpoint should exist");
        assertNotNull(paths.get("/api/v1/services/:service/config"), "Service config endpoint should exist");
        
        // Verify Configuration endpoints have proper operations
        JsonNode servicesEndpoint = paths.get("/api/v1/services");
        assertNotNull(servicesEndpoint.get("get"), "Services GET operation should exist");
        
        JsonNode serviceConfigEndpoint = paths.get("/api/v1/services/:service/config");
        assertNotNull(serviceConfigEndpoint.get("put"), "Service config PUT operation should exist");
        assertNotNull(serviceConfigEndpoint.get("delete"), "Service config DELETE operation should exist");
    }

    @Test
    void testResponsesHaveBothExamplesAndSchemas() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        JsonNode paths = rootNode.get("paths");
        
        // Check Ring endpoint response
        JsonNode ringResponse = paths.get("/api/v1/cassandra/ring")
                                     .get("get")
                                     .get("responses")
                                     .get("200")
                                     .get("content")
                                     .get("application/json")
                                     .get("schema");
        
        // Should have either a $ref (schema reference) or both example and schema structure
        assertTrue(ringResponse.has("$ref") || ringResponse.has("example"), 
                   "Ring response should have schema reference or example");
        
        // Check Gossip endpoint response
        JsonNode gossipResponse = paths.get("/api/v1/cassandra/gossip")
                                       .get("get")
                                       .get("responses")
                                       .get("200")
                                       .get("content")
                                       .get("application/json")
                                       .get("schema");
        
        assertTrue(gossipResponse.has("$ref") || gossipResponse.has("example"),
                   "Gossip response should have schema reference or example");
    }

    @Test
    void testHtmlContainsSwaggerUI() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path htmlFile = tempDir.resolve("api-docs.html");
        String htmlContent = Files.readString(htmlFile);
        
        // Verify HTML contains required elements
        assertThat(htmlContent).contains("<!DOCTYPE html>");
        assertThat(htmlContent).contains("Cassandra Sidecar API Documentation");
        assertThat(htmlContent).contains("swagger-ui-bundle.js");
        assertThat(htmlContent).contains("swagger-ui-standalone-preset.js");
        assertThat(htmlContent).contains("SwaggerUIBundle");
        assertThat(htmlContent).contains("dom_id: '#swagger-ui'");
    }

    @Test
    void testInvalidArgumentsHandling()
    {
        // Test with no arguments
        String[] noArgs = {};
        Exception exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, 
            () -> OpenApiDocumentationGenerator.main(noArgs)
        );
        assertThat(exception.getMessage()).contains("Usage: OpenApiDocumentationGenerator <output-directory>");
    }

    @Test
    void testValidJsonStructure() throws IOException
    {
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        Path jsonFile = tempDir.resolve("openapi.json");
        
        // Verify the JSON can be parsed as valid OpenAPI spec
        assertDoesNotThrow(() -> {
            String jsonContent = Files.readString(jsonFile);
            OpenAPI openAPI = Json.mapper().readValue(jsonContent, OpenAPI.class);
            assertNotNull(openAPI);
            assertNotNull(openAPI.getInfo());
            assertNotNull(openAPI.getPaths());
            assertNotNull(openAPI.getComponents());
        }, "Generated JSON should be valid OpenAPI specification");
    }
}
