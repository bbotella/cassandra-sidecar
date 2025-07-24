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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import org.apache.cassandra.sidecar.config.OpenApiConfiguration;
import org.apache.cassandra.sidecar.config.yaml.OpenApiConfigurationImpl;

/**
 * Utility class for generating OpenAPI documentation files
 */
public class OpenApiDocumentationGenerator
{
    private static final String HTML_TEMPLATE = 
        "<!DOCTYPE html>\n" +
        "<html lang=\"en\">\n" +
        "<head>\n" +
        "    <meta charset=\"UTF-8\">\n" +
        "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
        "    <title>Cassandra Sidecar API Documentation</title>\n" +
        "    <link rel=\"stylesheet\" type=\"text/css\" href=\"https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css\" />\n" +
        "    <style>\n" +
        "        html {\n" +
        "            box-sizing: border-box;\n" +
        "            overflow: -moz-scrollbars-vertical;\n" +
        "            overflow-y: scroll;\n" +
        "        }\n" +
        "        *, *:before, *:after {\n" +
        "            box-sizing: inherit;\n" +
        "        }\n" +
        "        body {\n" +
        "            margin:0;\n" +
        "            background: #fafafa;\n" +
        "        }\n" +
        "        .swagger-ui .topbar { display: none; }\n" +
        "        .swagger-ui .info { margin: 50px 0; }\n" +
        "        .swagger-ui .info hgroup.main { margin: 0 0 20px 0; }\n" +
        "        .swagger-ui .info h1 { color: #3b4151; }\n" +
        "    </style>\n" +
        "</head>\n" +
        "<body>\n" +
        "    <div id=\"swagger-ui\"></div>\n" +
        "    <script src=\"https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js\"></script>\n" +
        "    <script src=\"https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-standalone-preset.js\"></script>\n" +
        "    <script>\n" +
        "        const spec = %s;\n" +
        "        window.onload = function() {\n" +
        "            SwaggerUIBundle({\n" +
        "                spec: spec,\n" +
        "                dom_id: '#swagger-ui',\n" +
        "                deepLinking: true,\n" +
        "                presets: [\n" +
        "                    SwaggerUIBundle.presets.apis,\n" +
        "                    SwaggerUIStandalonePreset\n" +
        "                ],\n" +
        "                plugins: [\n" +
        "                    SwaggerUIBundle.plugins.DownloadUrl\n" +
        "                ],\n" +
        "                layout: \"StandaloneLayout\",\n" +
        "                defaultModelsExpandDepth: 1,\n" +
        "                defaultModelExpandDepth: 1\n" +
        "            });\n" +
        "        };\n" +
        "    </script>\n" +
        "</body>\n" +
        "</html>";

    /**
     * Generates OpenAPI documentation files
     * 
     * @param args command line arguments: outputDir
     */
    public static void main(String[] args) throws IOException
    {
        if (args.length < 1)
        {
            throw new IllegalArgumentException("Usage: OpenApiDocumentationGenerator <output-directory>");
        }

        String outputDir = args[0];
        Path outputPath = Paths.get(outputDir);
        
        // Create output directory if it doesn't exist
        Files.createDirectories(outputPath);
        
        // Generate OpenAPI specification
        OpenApiConfiguration config = new OpenApiConfigurationImpl();
        var openApi = createOpenApiFromConfig(config);
        
        // Scan for annotated handler classes
        openApi = scanForAnnotations(openApi);
        
        // Generate JSON file
        String jsonSpec = Json.pretty(openApi);
        Path jsonFile = outputPath.resolve("openapi.json");
        Files.write(jsonFile, jsonSpec.getBytes(StandardCharsets.UTF_8));
        System.out.printf("Generated: %s%n", jsonFile.toAbsolutePath());
        
        // Generate YAML file
        String yamlSpec = Yaml.pretty(openApi);
        Path yamlFile = outputPath.resolve("openapi.yaml");
        Files.write(yamlFile, yamlSpec.getBytes(StandardCharsets.UTF_8));
        System.out.printf("Generated: %s%n", yamlFile.toAbsolutePath());
        
        // Generate HTML file with embedded specification
        String htmlContent = HTML_TEMPLATE.replace("%s", jsonSpec);
        Path htmlFile = outputPath.resolve("api-docs.html");
        Files.write(htmlFile, htmlContent.getBytes(StandardCharsets.UTF_8));
        System.out.printf("Generated: %s%n", htmlFile.toAbsolutePath());
        
        System.out.printf("OpenAPI documentation generated successfully!%n");
        System.out.printf("Open %s in your browser to view the documentation.%n", htmlFile.toAbsolutePath());
    }
    
    /**
     * Creates an OpenAPI configuration from the given configuration
     */
    private static OpenAPI createOpenApiFromConfig(OpenApiConfiguration config)
    {
        OpenAPI openApi = new OpenAPI();
        
        // Set basic info
        io.swagger.v3.oas.models.info.Info info = new io.swagger.v3.oas.models.info.Info();
        info.setTitle(config.title());
        info.setDescription(config.description());
        info.setVersion(config.version());
        
        // Set license info
        io.swagger.v3.oas.models.info.License license = new io.swagger.v3.oas.models.info.License();
        license.setName(config.licenseName());
        license.setUrl(config.licenseUrl());
        info.setLicense(license);
        
        openApi.setInfo(info);
        
        // Set server info
        io.swagger.v3.oas.models.servers.Server server = new io.swagger.v3.oas.models.servers.Server();
        server.setUrl(config.serverUrl());
        server.setDescription(config.serverDescription());
        openApi.setServers(Collections.singletonList(server));
        
        return openApi;
    }
    
    /**
     * Scans for OpenAPI annotations in module classes
     * 
     * @param openApi base OpenAPI configuration
     * @return OpenAPI with discovered endpoints
     */
    private static OpenAPI scanForAnnotations(OpenAPI openApi)
    {
        try
        {
            // List of module classes to scan for route definitions
            Set<Class<?>> moduleClasses = Set.of(
                // Health check module
                org.apache.cassandra.sidecar.modules.HealthCheckModule.class,
                
                // Cassandra operations module  
                org.apache.cassandra.sidecar.modules.CassandraOperationsModule.class,
                
                // SSTable access module
                org.apache.cassandra.sidecar.modules.SSTablesAccessModule.class,
                
                // Restore jobs module
                org.apache.cassandra.sidecar.modules.RestoreJobModule.class,
                
                // CDC module
                org.apache.cassandra.sidecar.modules.CdcModule.class,
                
                // Live migration module  
                org.apache.cassandra.sidecar.modules.LiveMigrationModule.class,
                
                // OpenAPI module
                org.apache.cassandra.sidecar.modules.OpenApiModule.class
            );
            
            // Scan each module class for annotations and build OpenAPI paths
            io.swagger.v3.oas.models.Paths paths = new io.swagger.v3.oas.models.Paths();
            Map<String, io.swagger.v3.oas.models.tags.Tag> tags = new HashMap<>();
            Set<Class<?>> schemaClasses = new HashSet<>();
            
            for (Class<?> clazz : moduleClasses)
            {
                processModuleClass(clazz, paths, tags, schemaClasses);
            }
            
            // Set paths and tags to the OpenAPI object
            openApi.setPaths(paths);
            if (!tags.isEmpty())
            {
                openApi.setTags(tags.values().stream().collect(Collectors.toList()));
            }
            
            // Generate component schemas for referenced classes
            if (!schemaClasses.isEmpty())
            {
                Components components = new Components();
                Map<String, Schema> schemas = new HashMap<>();
                Set<Class<?>> processedClasses = new HashSet<>();
                
                // Add synthetic response classes for missing schemas
                addSyntheticResponseSchemas(schemas);
                
                // Process all schema classes and their nested dependencies
                while (!schemaClasses.isEmpty())
                {
                    Set<Class<?>> currentBatch = new HashSet<>(schemaClasses);
                    schemaClasses.clear();
                    
                    for (Class<?> schemaClass : currentBatch)
                    {
                        if (!processedClasses.contains(schemaClass))
                        {
                            Schema<?> schema = generateSchemaForClass(schemaClass, schemaClasses);
                            schemas.put(schemaClass.getSimpleName(), schema);
                            processedClasses.add(schemaClass);
                        }
                    }
                }
                
                components.setSchemas(schemas);
                openApi.setComponents(components);
            }
            else
            {
                // Even if no schema classes from annotations, add synthetic ones
                Components components = new Components();
                Map<String, Schema> schemas = new HashMap<>();
                addSyntheticResponseSchemas(schemas);
                components.setSchemas(schemas);
                openApi.setComponents(components);
            }
            
            return openApi;
        }
        catch (RuntimeException e)
        {
            // Log warning but continue with basic OpenAPI config
            return openApi;
        }
    }
    
    private static void processModuleClass(Class<?> clazz, io.swagger.v3.oas.models.Paths paths,
                                           Map<String, io.swagger.v3.oas.models.tags.Tag> tags, Set<Class<?>> schemaClasses)
    {
        // Scan methods for route provider methods with OpenAPI annotations
        for (Method method : clazz.getDeclaredMethods())
        {
            // Check if this is a route provider method (has @ProvidesIntoMap annotation)
            if (method.isAnnotationPresent(com.google.inject.multibindings.ProvidesIntoMap.class))
            {
                // Process OpenAPI annotations on the route provider method
                processRouteProviderMethod(method, paths, tags, schemaClasses);
            }
        }
    }
    
    private static void processRouteProviderMethod(Method method, io.swagger.v3.oas.models.Paths paths,
                                                   Map<String, io.swagger.v3.oas.models.tags.Tag> tags, Set<Class<?>> schemaClasses)
    {
        // Get class-level tag annotation
        Tag classTagAnnotation = method.getAnnotation(Tag.class);
        if (classTagAnnotation != null)
        {
            io.swagger.v3.oas.models.tags.Tag tag = new io.swagger.v3.oas.models.tags.Tag();
            tag.setName(classTagAnnotation.name());
            tag.setDescription(classTagAnnotation.description());
            tags.put(classTagAnnotation.name(), tag);
        }
        
        // Get Operation annotation
        Operation operationAnnotation = method.getAnnotation(Operation.class);
        if (operationAnnotation != null)
        {
            // Extract route key information to determine the actual HTTP path and method
            String pathName = extractPathFromRouteKey(method);
            String httpMethod = extractHttpMethodFromRouteKey(method);
            
            if (pathName != null && httpMethod != null)
            {
                PathItem pathItem = paths.get(pathName);
                if (pathItem == null)
                {
                    pathItem = new PathItem();
                    paths.addPathItem(pathName, pathItem);
                }
                
                // Create operation
                io.swagger.v3.oas.models.Operation operation = new io.swagger.v3.oas.models.Operation();
                operation.setSummary(operationAnnotation.summary());
                operation.setDescription(operationAnnotation.description());
                
                // Add tag if present
                if (classTagAnnotation != null)
                {
                    operation.addTagsItem(classTagAnnotation.name());
                }
                
                // Process ApiResponses
                io.swagger.v3.oas.annotations.responses.ApiResponses responsesAnnotation = 
                    method.getAnnotation(io.swagger.v3.oas.annotations.responses.ApiResponses.class);
                if (responsesAnnotation != null)
                {
                    io.swagger.v3.oas.models.responses.ApiResponses responses = new io.swagger.v3.oas.models.responses.ApiResponses();
                    for (io.swagger.v3.oas.annotations.responses.ApiResponse responseAnnotation : responsesAnnotation.value())
                    {
                        io.swagger.v3.oas.models.responses.ApiResponse response = new io.swagger.v3.oas.models.responses.ApiResponse();
                        response.setDescription(responseAnnotation.description());
                        
                        // Add content if specified
                        if (responseAnnotation.content().length > 0)
                        {
                            Content content = new Content();
                            for (io.swagger.v3.oas.annotations.media.Content contentAnnotation : responseAnnotation.content())
                            {
                                MediaType mediaType = new MediaType();
                                
                                // Process schema if present
                                io.swagger.v3.oas.annotations.media.Schema schemaAnnotation = contentAnnotation.schema();
                                if (schemaAnnotation != null)
                                {
                                    Schema<?> schema = new Schema<>();
                                    
                                    // Set schema implementation class if specified
                                    if (schemaAnnotation.implementation() != Void.class)
                                    {
                                        schema.set$ref("#/components/schemas/" + schemaAnnotation.implementation().getSimpleName());
                                        // Add the class to the set for schema generation
                                        schemaClasses.add(schemaAnnotation.implementation());
                                    }
                                    
                                    // Set schema example if provided
                                    if (!schemaAnnotation.example().isEmpty())
                                    {
                                        schema.setExample(schemaAnnotation.example());
                                    }
                                    
                                    // Set schema description if provided
                                    if (!schemaAnnotation.description().isEmpty())
                                    {
                                        schema.setDescription(schemaAnnotation.description());
                                    }
                                    
                                    // Set schema type if provided
                                    if (!schemaAnnotation.type().isEmpty())
                                    {
                                        schema.setType(schemaAnnotation.type());
                                    }
                                    
                                    mediaType.setSchema(schema);
                                }
                                
                                content.addMediaType(contentAnnotation.mediaType(), mediaType);
                            }
                            response.setContent(content);
                        }
                        
                        responses.addApiResponse(responseAnnotation.responseCode(), response);
                    }
                    operation.setResponses(responses);
                }
                
                // Add operation to path item based on HTTP method
                switch (httpMethod.toUpperCase())
                {
                    case "GET":
                        pathItem.setGet(operation);
                        break;
                    case "POST":
                        pathItem.setPost(operation);
                        break;
                    case "PUT":
                        pathItem.setPut(operation);
                        break;
                    case "DELETE":
                        pathItem.setDelete(operation);
                        break;
                    case "PATCH":
                        pathItem.setPatch(operation);
                        break;
                    case "HEAD":
                        pathItem.setHead(operation);
                        break;
                    case "OPTIONS":
                        pathItem.setOptions(operation);
                        break;
                    default:
                        // Default to GET if unknown
                        pathItem.setGet(operation);
                        break;
                }
            }
        }
    }
    
    /**
     * Extracts the HTTP path from the route key annotation
     */
    private static String extractPathFromRouteKey(Method method)
    {
        try
        {
            // Get the KeyClassMapKey annotation
            org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey keyAnnotation = 
                method.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey.class);
            
            if (keyAnnotation != null)
            {
                Class<?> keyClass = keyAnnotation.value();
                // Get the ROUTE_URI field from the route key class
                try
                {
                    java.lang.reflect.Field routeUriField = keyClass.getDeclaredField("ROUTE_URI");
                    routeUriField.setAccessible(true);
                    return (String) routeUriField.get(null);
                }
                catch (IllegalAccessException | NoSuchFieldException e)
                {
                    // If we can't get the route, return a fallback
                    return "/api/v1/" + method.getName().replace("Route", "").toLowerCase();
                }
            }
        }
        catch (Exception e)
        {
            // Ignore and return fallback
        }
        return null;
    }
    
    /**
     * Extracts the HTTP method from the route key annotation
     */
    private static String extractHttpMethodFromRouteKey(Method method)
    {
        try
        {
            // Get the KeyClassMapKey annotation
            org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey keyAnnotation = 
                method.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey.class);
            
            if (keyAnnotation != null)
            {
                Class<?> keyClass = keyAnnotation.value();
                // Get the HTTP_METHOD field from the route key class
                try
                {
                    java.lang.reflect.Field httpMethodField = keyClass.getDeclaredField("HTTP_METHOD");
                    httpMethodField.setAccessible(true);
                    Object httpMethodValue = httpMethodField.get(null);
                    return httpMethodValue.toString();
                }
                catch (IllegalAccessException | NoSuchFieldException e)
                {
                    // Default to GET if we can't determine the method
                    return "GET";
                }
            }
        }
        catch (Exception e)
        {
            // Ignore and return fallback
        }
        return "GET";
    }
    
    private static void processClass(Class<?> clazz, io.swagger.v3.oas.models.Paths paths,
                                     Map<String, io.swagger.v3.oas.models.tags.Tag> tags, Set<Class<?>> schemaClasses)
    {
        // Get class-level tag annotation
        Tag classTagAnnotation = clazz.getAnnotation(Tag.class);
        if (classTagAnnotation != null)
        {
            io.swagger.v3.oas.models.tags.Tag tag = new io.swagger.v3.oas.models.tags.Tag();
            tag.setName(classTagAnnotation.name());
            tag.setDescription(classTagAnnotation.description());
            tags.put(classTagAnnotation.name(), tag);
        }
        
        // Scan methods for Operation annotations
        for (Method method : clazz.getDeclaredMethods())
        {
            Operation operationAnnotation = method.getAnnotation(Operation.class);
            if (operationAnnotation != null)
            {
                // Create a simple path entry for demonstration
                // In a real implementation, you'd need to map handlers to actual HTTP paths
                String pathName = "/" + clazz.getSimpleName().toLowerCase().replace("handler", "");
                
                PathItem pathItem = paths.get(pathName);
                if (pathItem == null)
                {
                    pathItem = new PathItem();
                    paths.addPathItem(pathName, pathItem);
                }
                
                // Create operation
                io.swagger.v3.oas.models.Operation operation = new io.swagger.v3.oas.models.Operation();
                operation.setSummary(operationAnnotation.summary());
                operation.setDescription(operationAnnotation.description());
                
                // Add tag if present
                if (classTagAnnotation != null)
                {
                    operation.addTagsItem(classTagAnnotation.name());
                }
                
                // Process ApiResponses
                io.swagger.v3.oas.annotations.responses.ApiResponses responsesAnnotation = 
                    method.getAnnotation(io.swagger.v3.oas.annotations.responses.ApiResponses.class);
                if (responsesAnnotation != null)
                {
                    io.swagger.v3.oas.models.responses.ApiResponses responses = new io.swagger.v3.oas.models.responses.ApiResponses();
                    for (io.swagger.v3.oas.annotations.responses.ApiResponse responseAnnotation : responsesAnnotation.value())
                    {
                        io.swagger.v3.oas.models.responses.ApiResponse response = new io.swagger.v3.oas.models.responses.ApiResponse();
                        response.setDescription(responseAnnotation.description());
                        
                        // Add content if specified
                        if (responseAnnotation.content().length > 0)
                        {
                            Content content = new Content();
                            for (io.swagger.v3.oas.annotations.media.Content contentAnnotation : responseAnnotation.content())
                            {
                                MediaType mediaType = new MediaType();
                                
                                // Process schema if present
                                io.swagger.v3.oas.annotations.media.Schema schemaAnnotation = contentAnnotation.schema();
                                if (schemaAnnotation != null)
                                {
                                    Schema<?> schema = new Schema<>();
                                    
                                    // Set schema implementation class if specified
                                    if (schemaAnnotation.implementation() != Void.class)
                                    {
                                        schema.set$ref("#/components/schemas/" + schemaAnnotation.implementation().getSimpleName());
                                        // Add the class to the set for schema generation
                                        schemaClasses.add(schemaAnnotation.implementation());
                                    }
                                    
                                    // Set schema example if provided
                                    if (!schemaAnnotation.example().isEmpty())
                                    {
                                        schema.setExample(schemaAnnotation.example());
                                    }
                                    
                                    // Set schema description if provided
                                    if (!schemaAnnotation.description().isEmpty())
                                    {
                                        schema.setDescription(schemaAnnotation.description());
                                    }
                                    
                                    // Set schema type if provided
                                    if (!schemaAnnotation.type().isEmpty())
                                    {
                                        schema.setType(schemaAnnotation.type());
                                    }
                                    
                                    mediaType.setSchema(schema);
                                }
                                
                                content.addMediaType(contentAnnotation.mediaType(), mediaType);
                            }
                            response.setContent(content);
                        }
                        
                        responses.addApiResponse(responseAnnotation.responseCode(), response);
                    }
                    operation.setResponses(responses);
                }
                
                // Add operation to path (assuming GET for simplicity)
                pathItem.setGet(operation);
                
                // Add missing response schemas for endpoints that don't have proper annotations
                addMissingResponseSchemas(operation, clazz.getSimpleName(), schemaClasses);
            }
        }
    }
    
    /**
     * Adds missing response schemas for endpoints that don't have schema references
     */
    private static void addMissingResponseSchemas(io.swagger.v3.oas.models.Operation operation,
                                                  String handlerClassName, Set<Class<?>> schemaClasses)
    {
        io.swagger.v3.oas.models.responses.ApiResponses responses = operation.getResponses();
        if (responses != null)
        {
            io.swagger.v3.oas.models.responses.ApiResponse okResponse = responses.get("200");
            if (okResponse != null && (okResponse.getContent() == null || okResponse.getContent().isEmpty()))
            {
                // Add schema reference based on handler type
                String schemaName = getSchemaNameForHandler(handlerClassName);
                if (schemaName != null)
                {
                    Content content = new Content();
                    MediaType mediaType = new MediaType();
                    Schema<?> schema = new Schema<>();
                    schema.set$ref("#/components/schemas/" + schemaName);
                    mediaType.setSchema(schema);
                    content.addMediaType("application/json", mediaType);
                    okResponse.setContent(content);
                }
            }
        }
    }
    
    /**
     * Maps handler class names to appropriate response schema names
     */
    private static String getSchemaNameForHandler(String handlerClassName)
    {
        switch (handlerClassName)
        {
            case "RingHandler":
            case "KeyspaceRingHandler":
                return "RingResponse";
            case "SSTableCleanupHandler":
                return "SSTableCleanupResponse";
            case "CreateSnapshotHandler":
                return "CreateSnapshotResponse";
            case "ClearSnapshotHandler":
                return "ClearSnapshotResponse";
            case "ReportSchemaHandler":
                return "ReportSchemaResponse";
            case "RestoreJobProgressHandler":
                return "RestoreJobProgressResponsePayload";
            case "AbortRestoreJobHandler":
                return "AbortRestoreJobResponse";
            case "UpdateServiceConfigHandler":
                return "UpdateServiceConfigResponse";
            case "ListCdcDirHandler":
                return "ListCdcSegmentsResponse";
            case "NodeDecommissionHandler":
                return "OperationalJobResponse";
            case "SchemaHandler":
                return "SchemaResponse";
            default:
                return null;
        }
    }
    
    /**
     * Generates a schema definition for a given class by introspecting its fields
     */
    private static Schema<?> generateSchemaForClass(Class<?> clazz, Set<Class<?>> schemaClasses)
    {
        Schema<Object> schema = new Schema<>();
        schema.setType("object");  
        schema.setName(clazz.getSimpleName());
        
        Map<String, Schema> properties = new HashMap<>();
        
        // Process declared fields
        for (Field field : clazz.getDeclaredFields())
        {
            // Skip static and synthetic fields
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || field.isSynthetic())
            {
                continue;
            }
            
            Schema<?> propertySchema = generateSchemaForField(field, schemaClasses);
            properties.put(field.getName(), propertySchema);
        }
        
        schema.setProperties(properties);
        
        // Add examples based on known response types
        Object example = generateExampleForClass(clazz);
        if (example != null)
        {
            schema.setExample(example);
        }
        
        return schema;
    }
    
    /**
     * Generates a schema for a specific field based on its type
     */
    private static Schema<?> generateSchemaForField(Field field, Set<Class<?>> schemaClasses)
    {
        Schema<?> propertySchema = new Schema<>();
        Class<?> fieldType = field.getType();
        Type genericType = field.getGenericType();
        
        // Handle primitive and basic types
        if (fieldType == String.class)
        {
            propertySchema.setType("string");
        }
        else if (fieldType == Integer.class || fieldType == int.class)
        {
            propertySchema.setType("integer");
            propertySchema.setFormat("int32");
        }
        else if (fieldType == Long.class || fieldType == long.class)
        {
            propertySchema.setType("integer");
            propertySchema.setFormat("int64");
        }
        else if (fieldType == Boolean.class || fieldType == boolean.class)
        {
            propertySchema.setType("boolean");
        }
        else if (fieldType == Double.class || fieldType == double.class)
        {
            propertySchema.setType("number");
            propertySchema.setFormat("double");
        }
        else if (fieldType == Float.class || fieldType == float.class)
        {
            propertySchema.setType("number");
            propertySchema.setFormat("float");
        }
        else if (fieldType == UUID.class)
        {
            propertySchema.setType("string");
            propertySchema.setFormat("uuid");
        }
        else if (fieldType.isEnum())
        {
            propertySchema.setType("string");
            // Could add enum values here if needed
        }
        else if (List.class.isAssignableFrom(fieldType))
        {
            propertySchema.setType("array");
            // Handle generic type for items
            if (genericType instanceof ParameterizedType)
            {
                ParameterizedType paramType = (ParameterizedType) genericType;
                Type[] actualTypes = paramType.getActualTypeArguments();
                if (actualTypes.length > 0 && actualTypes[0] instanceof Class)
                {
                    Class<?> itemType = (Class<?>) actualTypes[0];
                    Schema<?> itemSchema = new Schema<>();
                    
                    if (itemType == String.class)
                    {
                        itemSchema.setType("string");
                    }
                    else if (itemType == Integer.class)
                    {
                        itemSchema.setType("integer");
                    }
                    else if (itemType == Long.class)
                    {
                        itemSchema.setType("integer");
                        itemSchema.setFormat("int64");
                    }
                    else
                    {
                        // Complex object in array - reference schema
                        itemSchema.set$ref("#/components/schemas/" + itemType.getSimpleName());
                        // Add the nested class for processing
                        schemaClasses.add(itemType);
                    }
                    
                    propertySchema.setItems(itemSchema);
                }
            }
        }
        else if (Map.class.isAssignableFrom(fieldType))
        {
            propertySchema.setType("object");
            // Handle generic types for additionalProperties
            if (genericType instanceof ParameterizedType)
            {
                ParameterizedType paramType = (ParameterizedType) genericType;
                Type[] actualTypes = paramType.getActualTypeArguments();
                if (actualTypes.length > 1 && actualTypes[1] instanceof Class)
                {
                    Class<?> valueType = (Class<?>) actualTypes[1];
                    Schema<?> additionalPropsSchema = new Schema<>();
                    
                    if (valueType == String.class)
                    {
                        additionalPropsSchema.setType("string");
                    }
                    else if (valueType == Long.class)
                    {
                        additionalPropsSchema.setType("integer");
                        additionalPropsSchema.setFormat("int64");
                    }
                    else
                    {
                        // Complex object as map value - reference schema
                        additionalPropsSchema.set$ref("#/components/schemas/" + valueType.getSimpleName());
                        // Add the nested class for processing
                        schemaClasses.add(valueType);
                    }
                    
                    propertySchema.setAdditionalProperties(additionalPropsSchema);
                }
            }
        }
        else
        {
            // Complex object - reference another schema
            propertySchema.set$ref("#/components/schemas/" + fieldType.getSimpleName());
            // Add the nested class for processing
            schemaClasses.add(fieldType);
        }
        
        return propertySchema;
    }
    
    /**
     * Generates example data for known response classes
     */
    private static Object generateExampleForClass(Class<?> clazz)
    {
        String className = clazz.getSimpleName();
        
        switch (className)
        {
            case "ListSnapshotFilesResponse":
                Map<String, Object> listSnapshotExample = new HashMap<>();
                Map<String, Object> fileInfoExample = new HashMap<>();
                fileInfoExample.put("size", 1048576L);
                fileInfoExample.put("host", "127.0.0.1");
                fileInfoExample.put("port", 9042);
                fileInfoExample.put("dataDirIndex", 0);
                fileInfoExample.put("snapshotName", "backup_20240101");
                fileInfoExample.put("keySpaceName", "test_keyspace");
                fileInfoExample.put("tableName", "test_table");
                fileInfoExample.put("fileName", "mc-1-big-Data.db");
                listSnapshotExample.put("snapshotFilesInfo", List.of(fileInfoExample));
                return listSnapshotExample;
                
            case "CreateRestoreJobResponsePayload":
                Map<String, Object> createJobExample = new HashMap<>();
                createJobExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                createJobExample.put("status", "CREATED");
                return createJobExample;
                
            case "UpdateRestoreJobResponsePayload":
                Map<String, Object> updateJobExample = new HashMap<>();
                updateJobExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                updateJobExample.put("status", "UPDATED");
                updateJobExample.put("message", "Job configuration updated");
                return updateJobExample;
                
            case "CreateRestoreSliceResponsePayload":
                Map<String, Object> createSliceExample = new HashMap<>();
                createSliceExample.put("sliceId", "slice-789");
                createSliceExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                createSliceExample.put("status", "CREATED");
                return createSliceExample;
                
            case "SSTableImportResponse":
                Map<String, Object> importExample = new HashMap<>();
                importExample.put("success", true);
                importExample.put("uploadId", "upload-123456");
                importExample.put("keyspace", "test_keyspace");
                importExample.put("tableName", "test_table");
                return importExample;
                
            case "SSTableUploadResponse":
                Map<String, Object> uploadExample = new HashMap<>();
                uploadExample.put("uploadId", "upload-789012");
                uploadExample.put("uploadSizeBytes", 2097152L);
                uploadExample.put("serviceTimeMillis", 1500L);
                return uploadExample;
                
            case "StreamStatsResponse":
                Map<String, Object> streamExample = new HashMap<>();
                streamExample.put("operationMode", "JOINING");
                Map<String, Object> statsExample = new HashMap<>();
                statsExample.put("totalFilesToReceive", 10L);
                statsExample.put("totalFilesReceived", 7L);
                statsExample.put("totalBytesToReceive", 104857600L);
                statsExample.put("totalBytesReceived", 73400320L);
                statsExample.put("totalFilesToSend", 5L);
                statsExample.put("totalFilesSent", 3L);
                statsExample.put("totalBytesToSend", 52428800L);
                statsExample.put("totalBytesSent", 31457280L);
                streamExample.put("streamsProgressStats", statsExample);
                return streamExample;
                
            case "GossipInfoResponse":
                Map<String, Object> gossipExample = new HashMap<>();
                Map<String, Object> nodeInfo = new HashMap<>();
                nodeInfo.put("generation", "1641024000"); 
                nodeInfo.put("heartbeat", "12345");
                nodeInfo.put("dc", "datacenter1");
                nodeInfo.put("rack", "rack1");
                nodeInfo.put("releaseVersion", "4.1.0");
                nodeInfo.put("schema", "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
                nodeInfo.put("load", "1024.5 MB");
                nodeInfo.put("hostId", "550e8400-e29b-41d4-a716-446655440000");
                gossipExample.put("/127.0.0.1:7000", nodeInfo);
                return gossipExample;
                
            case "SchemaResponse":
                Map<String, Object> schemaExample = new HashMap<>();
                schemaExample.put("keyspace", "test_keyspace");
                schemaExample.put("schema", "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
                return schemaExample;
                
            case "HealthResponse":
                return Collections.singletonMap("status", "OK");
                
            case "RingResponse":
                Map<String, Object> ringEntryExample = new HashMap<>();
                ringEntryExample.put("datacenter", "datacenter1");
                ringEntryExample.put("address", "127.0.0.1");
                ringEntryExample.put("port", 7000);
                ringEntryExample.put("rack", "rack1");
                ringEntryExample.put("status", "Up");
                ringEntryExample.put("state", "Normal");
                ringEntryExample.put("load", "1024.5 MB");
                ringEntryExample.put("owns", "33.33%");
                ringEntryExample.put("token", "1234567890123456789");
                ringEntryExample.put("fqdn", "node1.cluster.local");
                ringEntryExample.put("hostId", "550e8400-e29b-41d4-a716-446655440000");
                return List.of(ringEntryExample);
                
            case "TokenRangeReplicasResponse":
                Map<String, Object> rangeExample = new HashMap<>();
                rangeExample.put("writeReplicas", List.of("127.0.0.1:9042", "127.0.0.2:9042"));
                rangeExample.put("readReplicas", List.of("127.0.0.1:9042", "127.0.0.2:9042", "127.0.0.3:9042"));
                Map<String, Object> tokenRangeExample = new HashMap<>();
                tokenRangeExample.put("start", "0");
                tokenRangeExample.put("end", "1000000000000000000");
                rangeExample.put("tokenRange", tokenRangeExample);
                return rangeExample;
                
            case "RestoreJobProgressResponsePayload":
                Map<String, Object> progressExample = new HashMap<>();
                progressExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                progressExample.put("status", "IN_PROGRESS");
                progressExample.put("progressPercentage", 75.5);
                progressExample.put("message", "Restoring data files...");
                progressExample.put("startTime", "2024-01-01T10:00:00Z");
                progressExample.put("elapsedTime", "PT45M30S");
                return progressExample;
                
            case "RestoreJobSummaryResponsePayload":
                Map<String, Object> summaryExample = new HashMap<>();
                Map<String, Object> jobExample = new HashMap<>();
                jobExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                jobExample.put("status", "COMPLETED");
                jobExample.put("createdAt", "2024-01-01T10:00:00Z");
                jobExample.put("completedAt", "2024-01-01T11:30:00Z");
                summaryExample.put("jobs", List.of(jobExample));
                return summaryExample;
                
            case "ListCdcSegmentsResponse":
                Map<String, Object> cdcExample = new HashMap<>();
                Map<String, Object> segmentExample = new HashMap<>();
                segmentExample.put("filename", "CommitLog-7-1641024000000.log");
                segmentExample.put("size", 67108864L);
                segmentExample.put("createdDate", "2024-01-01T10:00:00Z");
                cdcExample.put("segments", List.of(segmentExample));
                return cdcExample;
                
            case "UpdateServiceConfigResponse":
                Map<String, Object> configExample = new HashMap<>();
                configExample.put("message", "Service configuration updated successfully");
                configExample.put("timestamp", "2024-01-01T10:00:00Z");
                return configExample;
                
            case "OperationalJobResponse":
                Map<String, Object> opExample = new HashMap<>();
                opExample.put("jobId", "decommission-456");
                opExample.put("operation", "DECOMMISSION");
                opExample.put("status", "STARTED");
                opExample.put("message", "Node decommission initiated");
                return opExample;
                
            case "InstanceFilesListResponse":
                Map<String, Object> filesExample = new HashMap<>();
                Map<String, Object> fileExample = new HashMap<>();
                fileExample.put("filename", "mc-1-big-Data.db");
                fileExample.put("size", 1048576L);
                fileExample.put("lastModified", "2024-01-01T10:00:00Z");
                filesExample.put("files", List.of(fileExample));
                return filesExample;
                
            case "CreateSnapshotResponse":
                Map<String, Object> createExample = new HashMap<>();
                createExample.put("result", "Success");
                createExample.put("snapshotName", "backup_20240101");
                createExample.put("keyspace", "test_keyspace");
                createExample.put("table", "test_table");
                return createExample;
                
            case "ClearSnapshotResponse":
                Map<String, Object> clearExample = new HashMap<>();
                clearExample.put("result", "Success");
                clearExample.put("message", "Snapshot cleared successfully");
                return clearExample;
                
            case "SSTableCleanupResponse":
                Map<String, Object> cleanupExample = new HashMap<>();
                cleanupExample.put("result", "Success");
                cleanupExample.put("cleanedFiles", 5);
                cleanupExample.put("freedSpace", "10MB");
                return cleanupExample;
                
            case "ReportSchemaResponse":
                Map<String, Object> reportExample = new HashMap<>();
                reportExample.put("result", "Success");
                reportExample.put("message", "Schema reported successfully");
                reportExample.put("timestamp", "2024-01-01T10:00:00Z");
                return reportExample;
                
            case "AbortRestoreJobResponse":
                Map<String, Object> abortExample = new HashMap<>();
                abortExample.put("jobId", "123e4567-e89b-12d3-a456-426614174000");
                abortExample.put("status", "ABORTED");
                abortExample.put("message", "Restore job aborted successfully");
                return abortExample;
                
            default:
                return null;
        }
    }
    
    /**
     * Adds synthetic response schemas for endpoints that don't have proper response classes
     */
    private static void addSyntheticResponseSchemas(Map<String, Schema> schemas)
    {
        // Ring responses
        Schema<Object> ringResponseSchema = new Schema<>();
        ringResponseSchema.setType("array");
        Schema<?> ringEntrySchema = new Schema<>();
        ringEntrySchema.setType("object");
        Map<String, Schema> ringEntryProps = new HashMap<>();
        ringEntryProps.put("datacenter", createStringSchema());
        ringEntryProps.put("address", createStringSchema());
        ringEntryProps.put("port", createIntSchema());
        ringEntryProps.put("rack", createStringSchema());
        ringEntryProps.put("status", createStringSchema());
        ringEntryProps.put("state", createStringSchema());
        ringEntryProps.put("load", createStringSchema());
        ringEntryProps.put("owns", createStringSchema());
        ringEntryProps.put("token", createStringSchema());
        ringEntryProps.put("fqdn", createStringSchema());
        ringEntryProps.put("hostId", createStringSchema());
        ringEntrySchema.setProperties(ringEntryProps);
        ringResponseSchema.setItems(ringEntrySchema);
        Object ringExample = generateExampleForClass(createDummyClass("RingResponse"));
        if (ringExample != null)
        {
            ringResponseSchema.setExample(ringExample);
        }
        schemas.put("RingResponse", ringResponseSchema);
        
        // Gossip info response schema
        Schema<Object> gossipResponseSchema = new Schema<>();
        gossipResponseSchema.setType("object");
        gossipResponseSchema.setAdditionalProperties(true);
        Schema<?> gossipEntrySchema = new Schema<>();
        gossipEntrySchema.setType("object");
        Map<String, Schema> gossipEntryProps = new HashMap<>();
        gossipEntryProps.put("generation", createStringSchema());
        gossipEntryProps.put("heartbeat", createStringSchema());
        gossipEntryProps.put("status", createStringSchema());
        gossipEntryProps.put("load", createStringSchema());
        gossipEntryProps.put("schema", createStringSchema());
        gossipEntryProps.put("datacenter", createStringSchema());
        gossipEntryProps.put("rack", createStringSchema());
        gossipEntryProps.put("releaseVersion", createStringSchema());
        gossipEntryProps.put("hostId", createStringSchema());
        gossipEntryProps.put("tokens", createStringSchema());
        gossipEntryProps.put("rpcReady", createBooleanSchema());
        gossipEntryProps.put("internalAddressAndPort", createStringSchema());
        gossipEntryProps.put("nativeAddressAndPort", createStringSchema());
        gossipEntryProps.put("statusWithPort", createStringSchema());
        gossipEntrySchema.setProperties(gossipEntryProps);
        gossipResponseSchema.setAdditionalProperties(gossipEntrySchema);
        Object gossipResponseExample = generateExampleForClass(createDummyClass("GossipInfoResponse"));
        if (gossipResponseExample != null)
        {
            gossipResponseSchema.setExample(gossipResponseExample);
        }
        schemas.put("GossipInfoResponse", gossipResponseSchema);
        
        // Simple success response schemas
        schemas.put("CreateSnapshotResponse", createSuccessResponseSchema("CreateSnapshotResponse"));
        schemas.put("ClearSnapshotResponse", createSuccessResponseSchema("ClearSnapshotResponse"));
        schemas.put("SSTableCleanupResponse", createSuccessResponseSchema("SSTableCleanupResponse"));
        schemas.put("ReportSchemaResponse", createSuccessResponseSchema("ReportSchemaResponse"));
        schemas.put("AbortRestoreJobResponse", createSuccessResponseSchema("AbortRestoreJobResponse"));
        schemas.put("UpdateServiceConfigResponse", createSuccessResponseSchema("UpdateServiceConfigResponse"));
        
        // Job/Progress response schemas
        Schema<Object> restoreProgressSchema = new Schema<>();
        restoreProgressSchema.setType("object");
        Map<String, Schema> progressProps = new HashMap<>();
        progressProps.put("jobId", createUuidSchema());
        progressProps.put("status", createStringSchema());
        progressProps.put("progressPercentage", createDoubleSchema());
        progressProps.put("message", createStringSchema());
        progressProps.put("startTime", createStringSchema());
        progressProps.put("elapsedTime", createStringSchema());
        restoreProgressSchema.setProperties(progressProps);
        Object progressExample = generateExampleForClass(createDummyClass("RestoreJobProgressResponsePayload"));
        if (progressExample != null)
        {
            restoreProgressSchema.setExample(progressExample);
        }
        schemas.put("RestoreJobProgressResponsePayload", restoreProgressSchema);
        
        // CDC responses
        Schema<Object> cdcSegmentsSchema = new Schema<>();
        cdcSegmentsSchema.setType("object");
        Schema<?> segmentArraySchema = new Schema<>();
        segmentArraySchema.setType("array");
        Schema<?> segmentSchema = new Schema<>();
        segmentSchema.setType("object");
        Map<String, Schema> segmentProps = new HashMap<>();
        segmentProps.put("filename", createStringSchema());
        segmentProps.put("size", createLongSchema());
        segmentProps.put("createdDate", createStringSchema());
        segmentSchema.setProperties(segmentProps);
        segmentArraySchema.setItems(segmentSchema);
        Map<String, Schema> cdcProps = new HashMap<>();
        cdcProps.put("segments", segmentArraySchema);
        cdcSegmentsSchema.setProperties(cdcProps);
        Object cdcExample = generateExampleForClass(createDummyClass("ListCdcSegmentsResponse"));
        if (cdcExample != null)
        {
            cdcSegmentsSchema.setExample(cdcExample);
        }
        schemas.put("ListCdcSegmentsResponse", cdcSegmentsSchema);
        
        // Operation response
        Schema<Object> operationResponseSchema = new Schema<>(); 
        operationResponseSchema.setType("object");
        Map<String, Schema> operationProps = new HashMap<>();
        operationProps.put("jobId", createStringSchema());
        operationProps.put("operation", createStringSchema());
        operationProps.put("status", createStringSchema());
        operationProps.put("message", createStringSchema());
        operationResponseSchema.setProperties(operationProps);
        Object operationExample = generateExampleForClass(createDummyClass("OperationalJobResponse"));
        if (operationExample != null)
        {
            operationResponseSchema.setExample(operationExample);
        }
        // Add more response schemas for endpoints using inline examples
        
        // SSTable Upload/Import responses
        Schema<Object> sstableUploadSchema = new Schema<>();
        sstableUploadSchema.setType("object");
        Map<String, Schema> uploadProps = new HashMap<>();
        uploadProps.put("uploadId", createStringSchema());
        uploadProps.put("uploadSizeBytes", createLongSchema());
        uploadProps.put("serviceTimeMillis", createLongSchema());
        sstableUploadSchema.setProperties(uploadProps);
        Object uploadExample = generateExampleForClass(createDummyClass("SSTableUploadResponse"));
        if (uploadExample != null)
        {
            sstableUploadSchema.setExample(uploadExample);
        }
        schemas.put("SSTableUploadResponse", sstableUploadSchema);
        
        Schema<Object> sstableImportSchema = new Schema<>();
        sstableImportSchema.setType("object");
        Map<String, Schema> importProps = new HashMap<>();
        importProps.put("success", createBooleanSchema());
        importProps.put("uploadId", createStringSchema());
        importProps.put("keyspace", createStringSchema());
        importProps.put("tableName", createStringSchema());
        sstableImportSchema.setProperties(importProps);
        Object importExample = generateExampleForClass(createDummyClass("SSTableImportResponse"));
        if (importExample != null)
        {
            sstableImportSchema.setExample(importExample);
        }
        schemas.put("SSTableImportResponse", sstableImportSchema);
        
        // Snapshot listing response
        Schema<Object> listSnapshotSchema = new Schema<>();
        listSnapshotSchema.setType("object");
        Map<String, Schema> listSnapshotProps = new HashMap<>();
        Schema<?> snapshotFilesArraySchema = new Schema<>();
        snapshotFilesArraySchema.setType("array");
        Schema<?> snapshotFileSchema = new Schema<>();
        snapshotFileSchema.setType("object");
        Map<String, Schema> snapshotFileProps = new HashMap<>();
        snapshotFileProps.put("size", createLongSchema());
        snapshotFileProps.put("host", createStringSchema());
        snapshotFileProps.put("port", createIntSchema());
        snapshotFileProps.put("dataDirIndex", createIntSchema());
        snapshotFileProps.put("snapshotName", createStringSchema());
        snapshotFileProps.put("keySpaceName", createStringSchema());
        snapshotFileProps.put("tableName", createStringSchema());
        snapshotFileProps.put("fileName", createStringSchema());
        snapshotFileSchema.setProperties(snapshotFileProps);
        snapshotFilesArraySchema.setItems(snapshotFileSchema);
        listSnapshotProps.put("snapshotFilesInfo", snapshotFilesArraySchema);
        listSnapshotSchema.setProperties(listSnapshotProps);
        Object listSnapshotExample = generateExampleForClass(createDummyClass("ListSnapshotFilesResponse"));
        if (listSnapshotExample != null)
        {
            listSnapshotSchema.setExample(listSnapshotExample);
        }
        schemas.put("ListSnapshotFilesResponse", listSnapshotSchema);
        
        // Stream Stats response
        Schema<Object> streamStatsSchema = new Schema<>();
        streamStatsSchema.setType("object");
        Map<String, Schema> streamProps = new HashMap<>();
        streamProps.put("operationMode", createStringSchema());
        Schema<?> streamsProgressSchema = new Schema<>();
        streamsProgressSchema.setType("object");
        Map<String, Schema> progressStatsProps = new HashMap<>();
        progressStatsProps.put("totalFilesToReceive", createLongSchema());
        progressStatsProps.put("totalFilesReceived", createLongSchema());
        progressStatsProps.put("totalBytesToReceive", createLongSchema());
        progressStatsProps.put("totalBytesReceived", createLongSchema());
        streamsProgressSchema.setProperties(progressStatsProps);
        streamProps.put("streamsProgressStats", streamsProgressSchema);
        streamStatsSchema.setProperties(streamProps);
        Object streamExample = generateExampleForClass(createDummyClass("StreamStatsResponse"));
        if (streamExample != null)
        {
            streamStatsSchema.setExample(streamExample);
        }
        schemas.put("StreamStatsResponse", streamStatsSchema);
        
        // Schema response
        Schema<Object> schemaResponseSchema = new Schema<>();
        schemaResponseSchema.setType("object");
        Map<String, Schema> schemaProps = new HashMap<>();
        schemaProps.put("keyspace", createStringSchema());
        schemaProps.put("schema", createStringSchema());
        schemaResponseSchema.setProperties(schemaProps);
        Object schemaExample = generateExampleForClass(createDummyClass("SchemaResponse"));
        if (schemaExample != null)
        {
            schemaResponseSchema.setExample(schemaExample);
        }
        schemas.put("SchemaResponse", schemaResponseSchema);
        
        // Additional GossipInfoResponse schema (already defined above but keeping for completeness)
        // Note: GossipInfoResponse schema was already defined earlier in this method
        
        // Restore Job responses
        Schema<Object> createRestoreJobSchema = new Schema<>();
        createRestoreJobSchema.setType("object");
        Map<String, Schema> createJobProps = new HashMap<>();
        createJobProps.put("jobId", createUuidSchema());
        createJobProps.put("status", createStringSchema());
        createRestoreJobSchema.setProperties(createJobProps);
        Object createJobExample = generateExampleForClass(createDummyClass("CreateRestoreJobResponsePayload"));
        if (createJobExample != null)
        {
            createRestoreJobSchema.setExample(createJobExample);
        }
        schemas.put("CreateRestoreJobResponsePayload", createRestoreJobSchema);
        
        Schema<Object> restoreJobSummarySchema = new Schema<>();
        restoreJobSummarySchema.setType("object");
        Map<String, Schema> summaryProps = new HashMap<>();
        Schema<?> jobsArraySchema = new Schema<>();
        jobsArraySchema.setType("array");
        Schema<?> jobSchema = new Schema<>();
        jobSchema.setType("object");
        Map<String, Schema> jobProps = new HashMap<>();
        jobProps.put("jobId", createUuidSchema());
        jobProps.put("status", createStringSchema());
        jobProps.put("createdAt", createStringSchema());
        jobProps.put("completedAt", createStringSchema());
        jobSchema.setProperties(jobProps);
        jobsArraySchema.setItems(jobSchema);
        summaryProps.put("jobs", jobsArraySchema);
        restoreJobSummarySchema.setProperties(summaryProps);
        Object summaryExample = generateExampleForClass(createDummyClass("RestoreJobSummaryResponsePayload"));
        if (summaryExample != null)
        {
            restoreJobSummarySchema.setExample(summaryExample);
        }
        schemas.put("RestoreJobSummaryResponsePayload", restoreJobSummarySchema);
        
        // Additional restore job response schemas
        Schema<Object> updateRestoreJobSchema = new Schema<>();
        updateRestoreJobSchema.setType("object");
        Map<String, Schema> updateJobProps = new HashMap<>();
        updateJobProps.put("jobId", createUuidSchema());
        updateJobProps.put("status", createStringSchema());
        updateJobProps.put("message", createStringSchema());
        updateRestoreJobSchema.setProperties(updateJobProps);
        Object updateJobExample = generateExampleForClass(createDummyClass("UpdateRestoreJobResponsePayload"));
        if (updateJobExample != null)
        {
            updateRestoreJobSchema.setExample(updateJobExample);
        }
        schemas.put("UpdateRestoreJobResponsePayload", updateRestoreJobSchema);
        
        Schema<Object> createRestoreSliceSchema = new Schema<>();
        createRestoreSliceSchema.setType("object");
        Map<String, Schema> createSliceProps = new HashMap<>();
        createSliceProps.put("sliceId", createStringSchema());
        createSliceProps.put("jobId", createUuidSchema());
        createSliceProps.put("status", createStringSchema());
        createRestoreSliceSchema.setProperties(createSliceProps);
        Object createSliceExample = generateExampleForClass(createDummyClass("CreateRestoreSliceResponsePayload"));
        if (createSliceExample != null)
        {
            createRestoreSliceSchema.setExample(createSliceExample);
        }
        schemas.put("CreateRestoreSliceResponsePayload", createRestoreSliceSchema);
        
        // Service configuration responses
        Schema<Object> serviceConfigListSchema = new Schema<>();
        serviceConfigListSchema.setType("object");
        Map<String, Schema> configListProps = new HashMap<>();
        Schema<?> configurationsArraySchema = new Schema<>();
        configurationsArraySchema.setType("array");
        Schema<?> configSchema = new Schema<>();
        configSchema.setType("object");
        Map<String, Schema> configProps = new HashMap<>();
        configProps.put("key", createStringSchema());
        configProps.put("value", createStringSchema());
        configSchema.setProperties(configProps);
        configurationsArraySchema.setItems(configSchema);
        configListProps.put("configurations", configurationsArraySchema);
        serviceConfigListSchema.setProperties(configListProps);
        schemas.put("ServiceConfigListResponse", serviceConfigListSchema);
        
        // Live migration responses  
        Schema<Object> instanceFilesSchema = new Schema<>();
        instanceFilesSchema.setType("object");
        Map<String, Schema> filesProps = new HashMap<>();
        Schema<?> filesArraySchema = new Schema<>();
        filesArraySchema.setType("array");
        Schema<?> fileSchema = new Schema<>();
        fileSchema.setType("object");
        Map<String, Schema> fileProps = new HashMap<>();
        fileProps.put("filename", createStringSchema());
        fileProps.put("size", createLongSchema());
        fileProps.put("lastModified", createStringSchema());
        fileSchema.setProperties(fileProps);
        filesArraySchema.setItems(fileSchema);
        filesProps.put("files", filesArraySchema);
        instanceFilesSchema.setProperties(filesProps);
        Object filesExample = generateExampleForClass(createDummyClass("InstanceFilesListResponse"));
        if (filesExample != null)
        {
            instanceFilesSchema.setExample(filesExample);
        }
        schemas.put("InstanceFilesListResponse", instanceFilesSchema);
    }
    
    private static Schema<?> createStringSchema()
    {
        Schema<?> schema = new Schema<>();
        schema.setType("string");
        return schema;
    }
    
    private static Schema<?> createIntSchema()
    {
        Schema<?> schema = new Schema<>();
        schema.setType("integer");
        schema.setFormat("int32");
        return schema;
    }
    
    private static Schema<?> createLongSchema()
    {
        Schema<?> schema = new Schema<>(); 
        schema.setType("integer");
        schema.setFormat("int64");
        return schema;
    }
    
    private static Schema<?> createDoubleSchema()
    {
        Schema<?> schema = new Schema<>();
        schema.setType("number");
        schema.setFormat("double");
        return schema;
    }
    
    private static Schema<?> createBooleanSchema()
    {
        Schema<?> schema = new Schema<>();
        schema.setType("boolean");
        return schema;
    }
    
    private static Schema<?> createUuidSchema()
    {
        Schema<?> schema = new Schema<>();
        schema.setType("string");
        schema.setFormat("uuid");
        return schema;
    }
    
    private static Schema<Object> createSuccessResponseSchema(String className)
    {
        Schema<Object> schema = new Schema<>();
        schema.setType("object");
        Map<String, Schema> props = new HashMap<>();
        props.put("result", createStringSchema());
        props.put("message", createStringSchema());
        schema.setProperties(props);
        Object example = generateExampleForClass(createDummyClass(className));
        if (example != null)
        {
            schema.setExample(example);
        }
        return schema;
    }
    
    private static Class<?> createDummyClass(String className)
    {
        // Create a simple class reference for example generation
        switch (className)
        {
            case "RingResponse": return java.util.List.class;
            default: return java.util.Map.class;
        }
    }
}
