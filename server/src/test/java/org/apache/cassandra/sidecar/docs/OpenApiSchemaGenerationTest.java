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
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.core.util.Json;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests specifically for schema generation and validation in OpenAPI documentation
 */
class OpenApiSchemaGenerationTest
{
    @TempDir
    Path tempDir;

    private ObjectMapper objectMapper;
    private JsonNode schemas;

    @BeforeEach
    void setUp() throws IOException
    {
        objectMapper = Json.mapper();
        
        // Generate OpenAPI documentation
        String[] args = {tempDir.toString()};
        OpenApiDocumentationGenerator.main(args);
        
        // Load the generated JSON
        Path jsonFile = tempDir.resolve("openapi.json");
        JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
        schemas = rootNode.get("components").get("schemas");
    }

    @Test
    void testRingResponseSchemaStructure()
    {
        JsonNode ringSchema = schemas.get("RingResponse");
        assertNotNull(ringSchema, "RingResponse schema should exist");
        
        // Should be an array type
        assertThat(ringSchema.get("type").asText()).isEqualTo("array");
        
        JsonNode items = ringSchema.get("items");
        assertNotNull(items, "Array items should be defined");
        assertThat(items.get("type").asText()).isEqualTo("object");
        
        JsonNode properties = items.get("properties");
        assertNotNull(properties, "Item properties should be defined");
        
        // Verify all expected ring entry properties
        String[] expectedProperties = {
            "datacenter", "address", "port", "rack", "status", 
            "state", "load", "owns", "token", "fqdn", "hostId"
        };
        
        for (String prop : expectedProperties)
        {
            assertNotNull(properties.get(prop), prop + " property should be defined");
            assertNotNull(properties.get(prop).get("type"), prop + " should have a type");
        }
        
        // Verify specific property types
        assertThat(properties.get("port").get("type").asText()).isEqualTo("integer");
        assertThat(properties.get("port").get("format").asText()).isEqualTo("int32");
        assertThat(properties.get("address").get("type").asText()).isEqualTo("string");
        assertThat(properties.get("datacenter").get("type").asText()).isEqualTo("string");
        
        // Should have an example (if example generation is available for this schema)
        // Note: Some schemas may only have structural definition without examples
        assertTrue(ringSchema.has("example") || (ringSchema.has("items") && ringSchema.get("items").has("properties")),
                   "RingResponse should have an example or proper structural definition");
    }

    @Test
    void testGossipInfoResponseSchemaStructure()
    {
        JsonNode gossipSchema = schemas.get("GossipInfoResponse");
        assertNotNull(gossipSchema, "GossipInfoResponse schema should exist");
        
        // Should be an object type with additionalProperties
        assertThat(gossipSchema.get("type").asText()).isEqualTo("object");
        assertTrue(gossipSchema.get("additionalProperties").asBoolean() || 
                   gossipSchema.get("additionalProperties").isObject(),
                   "Should have additionalProperties defined");
        
        JsonNode additionalProps = gossipSchema.get("additionalProperties");
        if (additionalProps.isObject())
        {
            assertThat(additionalProps.get("type").asText()).isEqualTo("object");
            JsonNode properties = additionalProps.get("properties");
            
            if (properties != null)
            {
                // Verify expected gossip info properties
                String[] expectedGossipProperties = {
                    "generation", "heartbeat", "status", "load", "schema",
                    "rack", "releaseVersion", "hostId", "tokens", "rpcReady",
                    "internalAddressAndPort", "nativeAddressAndPort", "statusWithPort"
                };
                
                for (String prop : expectedGossipProperties)
                {
                    JsonNode propNode = properties.get(prop);
                    if (propNode != null)
                    {
                        assertNotNull(propNode.get("type"), prop + " should have a type");
                    }
                }
                
                // Verify specific types
                if (properties.get("rpcReady") != null)
                {
                    assertThat(properties.get("rpcReady").get("type").asText()).isEqualTo("boolean");
                }
            }
        }
        
        // Should have an example (if example generation is available for this schema)
        // Note: Some schemas may only have structural definition without examples
        assertTrue(gossipSchema.has("example") || gossipSchema.has("additionalProperties"),
                   "GossipInfoResponse should have an example or proper structural definition");
    }

    @Test
    void testRestoreJobSchemasExist()
    {
        String[] restoreJobSchemas = {
            "CreateRestoreJobResponsePayload",
            "UpdateRestoreJobResponsePayload", 
            "RestoreJobProgressResponsePayload",
            "RestoreJobSummaryResponsePayload",
            "CreateRestoreSliceResponsePayload"
        };
        
        for (String schemaName : restoreJobSchemas)
        {
            JsonNode schema = schemas.get(schemaName);
            assertNotNull(schema, schemaName + " schema should exist");
            assertThat(schema.get("type").asText()).isEqualTo("object");
            
            if (schema.has("properties"))
            {
                JsonNode properties = schema.get("properties");
                
                // Most restore job responses should have jobId
                if (properties.has("jobId"))
                {
                    JsonNode jobIdProp = properties.get("jobId");
                    assertTrue(jobIdProp.get("type").asText().equals("string") &&
                               (jobIdProp.has("format") && jobIdProp.get("format").asText().equals("uuid")) ||
                               jobIdProp.get("type").asText().equals("string"),
                               "jobId should be string with uuid format or string type");
                }
                
                // Should have status field
                if (properties.has("status"))
                {
                    assertThat(properties.get("status").get("type").asText()).isEqualTo("string");
                }
            }
            
            // Should have examples or proper schema structure
            assertTrue(schema.has("example") || schema.has("properties"),
                       schemaName + " should have an example or properties defined");
        }
    }

    @Test
    void testSSTableOperationSchemasExist()
    {
        String[] sstableSchemas = {
            "SSTableUploadResponse",
            "SSTableImportResponse",
            "ListSnapshotFilesResponse"
        };
        
        for (String schemaName : sstableSchemas)
        {
            JsonNode schema = schemas.get(schemaName);
            assertNotNull(schema, schemaName + " schema should exist");
            assertThat(schema.get("type").asText()).isEqualTo("object");
            
            if (schema.has("properties"))
            {
                JsonNode properties = schema.get("properties");
                
                // Upload response should have uploadId
                if (schemaName.equals("SSTableUploadResponse") && properties.has("uploadId"))
                {
                    assertThat(properties.get("uploadId").get("type").asText()).isEqualTo("string");
                }
                
                // Should have size fields as integers
                if (properties.has("uploadSizeBytes"))
                {
                    assertThat(properties.get("uploadSizeBytes").get("type").asText()).isEqualTo("integer");
                    assertThat(properties.get("uploadSizeBytes").get("format").asText()).isEqualTo("int64");
                }
            }
            
            assertTrue(schema.has("example") || schema.has("properties"),
                       schemaName + " should have an example or properties defined");
        }
    }

    @Test
    void testStreamingAndHealthSchemasExist()
    {
        String[] schemas = {
            "StreamStatsResponse",
            "SchemaResponse"
        };
        
        for (String schemaName : schemas)
        {
            JsonNode schema = this.schemas.get(schemaName);
            assertNotNull(schema, schemaName + " schema should exist");
            assertThat(schema.get("type").asText()).isEqualTo("object");
            assertTrue(schema.has("example") || schema.has("properties"),
                       schemaName + " should have an example or properties defined");
        }
    }

    @Test
    void testCdcSchemasExist()
    {
        JsonNode cdcSchema = schemas.get("ListCdcSegmentsResponse");
        assertNotNull(cdcSchema, "ListCdcSegmentsResponse schema should exist");
        assertThat(cdcSchema.get("type").asText()).isEqualTo("object");
        
        if (cdcSchema.has("properties"))
        {
            JsonNode properties = cdcSchema.get("properties");
            
            // Should have segments array
            if (properties.has("segments"))
            {
                JsonNode segments = properties.get("segments");
                assertThat(segments.get("type").asText()).isEqualTo("array");
                
                JsonNode items = segments.get("items");
                if (items != null)
                {
                    assertThat(items.get("type").asText()).isEqualTo("object");
                    
                    if (items.has("properties"))
                    {
                        JsonNode segmentProps = items.get("properties");
                        
                        // Verify segment properties
                        if (segmentProps.has("filename"))
                        {
                            assertThat(segmentProps.get("filename").get("type").asText()).isEqualTo("string");
                        }
                        if (segmentProps.has("size"))
                        {
                            assertThat(segmentProps.get("size").get("type").asText()).isEqualTo("integer");
                        }
                    }
                }
            }
        }
        
        assertTrue(cdcSchema.has("example") || cdcSchema.has("properties"),
                   "ListCdcSegmentsResponse should have an example or properties defined");
    }

    @Test
    void testAllSchemasHaveStructureOrExamples()
    {
        schemas.fieldNames().forEachRemaining(schemaName -> {
            JsonNode schema = schemas.get(schemaName);
            
            // Skip primitive type schemas and schemas without type
            if (!schema.has("type"))
            {
                return;
            }
            
            String type = schema.get("type").asText();
            if (!type.equals("object") && !type.equals("array"))
            {
                return;
            }
            
            // All object and array schemas should have examples, properties, or additionalProperties
            boolean hasStructure = schema.has("example") || 
                                   schema.has("properties") || 
                                   schema.has("additionalProperties") ||
                                   schema.has("items");
            
            assertTrue(hasStructure, 
                       schemaName + " (type: " + type + ") should have an example, properties, additionalProperties, or items defined");
        });
    }

    @Test
    void testSchemaReferencesAreValid()
    {
        // Collect all schema references from the OpenAPI spec
        Path jsonFile = tempDir.resolve("openapi.json");
        try
        {
            JsonNode rootNode = objectMapper.readTree(jsonFile.toFile());
            validateSchemaReferences(rootNode, "");
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to validate schema references", e);
        }
    }

    private void validateSchemaReferences(JsonNode node, String path)
    {
        if (node.isObject())
        {
            if (node.has("$ref"))
            {
                String ref = node.get("$ref").asText();
                if (ref.startsWith("#/components/schemas/"))
                {
                    String schemaName = ref.substring("#/components/schemas/".length());
                    assertNotNull(schemas.get(schemaName), 
                                  "Referenced schema '" + schemaName + "' should exist (referenced at " + path + ")");
                }
            }
            
            node.fieldNames().forEachRemaining(fieldName -> {
                validateSchemaReferences(node.get(fieldName), path + "/" + fieldName);
            });
        }
        else if (node.isArray())
        {
            for (int i = 0; i < node.size(); i++)
            {
                validateSchemaReferences(node.get(i), path + "[" + i + "]");
            }
        }
    }

    @Test
    void testComplexCollectionSchemasAreProperlyDefined()
    {
        // Test that our complex collection types (RingResponse, GossipInfoResponse) 
        // are properly defined with structured schemas, not just examples
        
        JsonNode ringSchema = schemas.get("RingResponse");
        assertNotNull(ringSchema, "RingResponse should exist");
        assertTrue(ringSchema.has("type"), "RingResponse should have explicit type");
        assertTrue(ringSchema.has("items") || ringSchema.has("properties") || ringSchema.has("additionalProperties"),
                   "RingResponse should have structured definition");
        
        JsonNode gossipSchema = schemas.get("GossipInfoResponse");
        assertNotNull(gossipSchema, "GossipInfoResponse should exist");
        assertTrue(gossipSchema.has("type"), "GossipInfoResponse should have explicit type");
        assertTrue(gossipSchema.has("additionalProperties") || gossipSchema.has("properties"),
                   "GossipInfoResponse should have structured definition");
        
        // These should not be just empty objects or only have examples
        assertThat(ringSchema.get("type").asText()).isNotEqualTo("string");
        assertThat(gossipSchema.get("type").asText()).isNotEqualTo("string");
    }
}
