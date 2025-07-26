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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.MediaType;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.config.OpenApiConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Handler that serves the OpenAPI specification
 */
@Tag(name = "Documentation", description = "API documentation endpoints")
@Singleton
public class OpenApiHandler implements Handler<RoutingContext>
{
    private final OpenApiConfiguration openApiConfig;

    @Inject
    public OpenApiHandler(SidecarConfiguration sidecarConfiguration)
    {
        this.openApiConfig = sidecarConfiguration.openApiConfiguration();
    }

    @Operation(
        summary = "Get OpenAPI specification",
        description = "Returns the OpenAPI specification for this API in JSON format"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "OpenAPI specification retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = OpenAPI.class)
            )
        )
    })
    @Override
    public void handle(RoutingContext context)
    {
        OpenAPI openAPI = createOpenApiFromConfig(openApiConfig);
        
        // Scan for annotated endpoints and add them to the OpenAPI spec
        openAPI = scanForAnnotations(openAPI);
        
        String openApiJson = Json.pretty(openAPI);
        
        context.response()
               .putHeader("Content-Type", "application/json")
               .end(openApiJson);
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
     * Scans for OpenAPI annotations in VertxRouteMapKeys using the embedded metadata
     */
    private static OpenAPI scanForAnnotations(OpenAPI openApi)
    {
        try
        {
            // Scan VertxRouteMapKeys interface for route definitions and build OpenAPI paths
            io.swagger.v3.oas.models.Paths paths = new io.swagger.v3.oas.models.Paths();
            Map<String, io.swagger.v3.oas.models.tags.Tag> tags = new HashMap<>();
            Set<Class<?>> schemaClasses = new HashSet<>();
            
            processVertxRouteMapKeysInterface(org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class, paths, tags, schemaClasses);
            
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
                Map<String, io.swagger.v3.oas.models.media.Schema> schemas = new HashMap<>();
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
                            io.swagger.v3.oas.models.media.Schema<?> schema = generateSchemaForClass(schemaClass, schemaClasses);
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
                Map<String, io.swagger.v3.oas.models.media.Schema> schemas = new HashMap<>();
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
    
    private static void processVertxRouteMapKeysInterface(Class<?> vertxRouteMapKeysClass, io.swagger.v3.oas.models.Paths paths,
                                                          Map<String, io.swagger.v3.oas.models.tags.Tag> tags, Set<Class<?>> schemaClasses)
    {
        // Scan inner interfaces for route key definitions with OpenAPI annotations
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            // Check if this is a route key interface (extends RouteClassKey) with OpenAPI annotations
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    // Process OpenAPI annotation for this route key interface
                    processRouteKeyInterface(innerClass, endpointAnnotation, paths, tags, schemaClasses);
                }
            }
        }
    }
    
    private static void processRouteKeyInterface(Class<?> routeKeyInterface, 
                                                 org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation, 
                                                 io.swagger.v3.oas.models.Paths paths,
                                                 Map<String, io.swagger.v3.oas.models.tags.Tag> tags, Set<Class<?>> schemaClasses)
    {
        // Create tag from annotation
        if (endpointAnnotation.tag() != null && !endpointAnnotation.tag().isEmpty())
        {
            io.swagger.v3.oas.models.tags.Tag tag = new io.swagger.v3.oas.models.tags.Tag();
            tag.setName(endpointAnnotation.tag());
            tag.setDescription(endpointAnnotation.tagDescription());
            tags.put(endpointAnnotation.tag(), tag);
        }
        
        // Extract route key information to determine the actual HTTP path and method
        String pathName = extractPathFromRouteKeyInterface(routeKeyInterface);
        String httpMethod = extractHttpMethodFromRouteKeyInterface(routeKeyInterface);
        
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
            operation.setSummary(endpointAnnotation.summary());
            operation.setDescription(endpointAnnotation.description());
            
            // Add tag if present
            if (endpointAnnotation.tag() != null && !endpointAnnotation.tag().isEmpty())
            {
                operation.addTagsItem(endpointAnnotation.tag());
            }
            
            // Process responses from annotation
            org.apache.cassandra.sidecar.modules.multibindings.OpenApiResponse[] responses = endpointAnnotation.responses();
            if (responses != null && responses.length > 0)
            {
                io.swagger.v3.oas.models.responses.ApiResponses apiResponses = new io.swagger.v3.oas.models.responses.ApiResponses();
                
                for (org.apache.cassandra.sidecar.modules.multibindings.OpenApiResponse responseAnnotation : responses)
                {
                    io.swagger.v3.oas.models.responses.ApiResponse response = new io.swagger.v3.oas.models.responses.ApiResponse();
                    response.setDescription(responseAnnotation.description());
                    
                    if (responseAnnotation.mediaType() != null && !responseAnnotation.mediaType().isEmpty() && 
                        (responseAnnotation.schemaClass() != Void.class || 
                         (responseAnnotation.schemaRef() != null && !responseAnnotation.schemaRef().isEmpty()) || 
                         (responseAnnotation.schemaType() != null && !responseAnnotation.schemaType().isEmpty())))
                    {
                        io.swagger.v3.oas.models.media.Content content = new io.swagger.v3.oas.models.media.Content();
                        MediaType mediaType = new MediaType();
                        
                        io.swagger.v3.oas.models.media.Schema<?> schema = new io.swagger.v3.oas.models.media.Schema<>();
                        if (responseAnnotation.schemaRef() != null && !responseAnnotation.schemaRef().isEmpty())
                        {
                            schema.set$ref(responseAnnotation.schemaRef());
                        }
                        else if (responseAnnotation.schemaClass() != Void.class)
                        {
                            schema.set$ref("#/components/schemas/" + responseAnnotation.schemaClass().getSimpleName());
                            schemaClasses.add(responseAnnotation.schemaClass());
                        }
                        
                        if (responseAnnotation.example() != null && !responseAnnotation.example().isEmpty())
                        {
                            schema.setExample(responseAnnotation.example());
                        }
                        
                        if (responseAnnotation.schemaType() != null && !responseAnnotation.schemaType().isEmpty())
                        {
                            schema.setType(responseAnnotation.schemaType());
                        }
                        
                        mediaType.setSchema(schema);
                        content.addMediaType(responseAnnotation.mediaType(), mediaType);
                        response.setContent(content);
                    }
                    
                    apiResponses.addApiResponse(responseAnnotation.responseCode(), response);
                }
                operation.setResponses(apiResponses);
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
    
    private static String extractPathFromRouteKeyInterface(Class<?> routeKeyInterface)
    {
        try
        {
            // Get the ROUTE_URI field from the route key interface
            Field routeUriField = routeKeyInterface.getDeclaredField("ROUTE_URI");
            routeUriField.setAccessible(true);
            return (String) routeUriField.get(null);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            // If we can't get the route, return a fallback
            return "/api/v1/" + routeKeyInterface.getSimpleName().replace("RouteKey", "").toLowerCase();
        }
    }
    
    private static String extractHttpMethodFromRouteKeyInterface(Class<?> routeKeyInterface)
    {
        try
        {
            // Get the HTTP_METHOD field from the route key interface
            Field httpMethodField = routeKeyInterface.getDeclaredField("HTTP_METHOD");
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
    
    private static io.swagger.v3.oas.models.media.Schema<?> generateSchemaForClass(Class<?> clazz, Set<Class<?>> schemaClasses)
    {
        io.swagger.v3.oas.models.media.Schema<Object> schema = new io.swagger.v3.oas.models.media.Schema<>();
        schema.setType("object");  
        schema.setName(clazz.getSimpleName());
        
        Map<String, io.swagger.v3.oas.models.media.Schema> properties = new HashMap<>();
        
        // Process declared fields
        for (Field field : clazz.getDeclaredFields())
        {
            // Skip static and synthetic fields
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || field.isSynthetic())
            {
                continue;
            }
            
            io.swagger.v3.oas.models.media.Schema<?> propertySchema = generateSchemaForField(field, schemaClasses);
            properties.put(field.getName(), propertySchema);
        }
        
        schema.setProperties(properties);
        
        return schema;
    }
    
    private static io.swagger.v3.oas.models.media.Schema<?> generateSchemaForField(Field field, Set<Class<?>> schemaClasses)
    {
        io.swagger.v3.oas.models.media.Schema<?> propertySchema = new io.swagger.v3.oas.models.media.Schema<>();
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
                    io.swagger.v3.oas.models.media.Schema<?> itemSchema = new io.swagger.v3.oas.models.media.Schema<>();
                    
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
                    io.swagger.v3.oas.models.media.Schema<?> additionalPropsSchema = new io.swagger.v3.oas.models.media.Schema<>();
                    
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
            schemaClasses.add(fieldType);
        }
        
        return propertySchema;
    }
    
    private static void addSyntheticResponseSchemas(Map<String, io.swagger.v3.oas.models.media.Schema> schemas)
    {
        // Ring responses
        io.swagger.v3.oas.models.media.Schema<Object> ringResponseSchema = new io.swagger.v3.oas.models.media.Schema<>();
        ringResponseSchema.setType("array");
        io.swagger.v3.oas.models.media.Schema<?> ringEntrySchema = new io.swagger.v3.oas.models.media.Schema<>();
        ringEntrySchema.setType("object");
        Map<String, io.swagger.v3.oas.models.media.Schema> ringEntryProps = new HashMap<>();
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
        schemas.put("RingResponse", ringResponseSchema);
        
        // Simple success response schemas
        schemas.put("CreateSnapshotResponse", createSuccessResponseSchema());
        schemas.put("ClearSnapshotResponse", createSuccessResponseSchema());
        schemas.put("SSTableCleanupResponse", createSuccessResponseSchema());
        schemas.put("ReportSchemaResponse", createSuccessResponseSchema());
        schemas.put("AbortRestoreJobResponse", createSuccessResponseSchema());
        schemas.put("UpdateServiceConfigResponse", createSuccessResponseSchema());
        
        // Add other synthetic schemas as needed
        schemas.put("GossipInfoResponse", createObjectSchema());
        schemas.put("RestoreJobProgressResponsePayload", createObjectSchema());
        schemas.put("ListCdcSegmentsResponse", createObjectSchema());
        schemas.put("OperationalJobResponse", createObjectSchema());
        schemas.put("SchemaResponse", createObjectSchema());
    }
    
    private static io.swagger.v3.oas.models.media.Schema<?> createStringSchema()
    {
        io.swagger.v3.oas.models.media.Schema<?> schema = new io.swagger.v3.oas.models.media.Schema<>();
        schema.setType("string");
        return schema;
    }
    
    private static io.swagger.v3.oas.models.media.Schema<?> createIntSchema()
    {
        io.swagger.v3.oas.models.media.Schema<?> schema = new io.swagger.v3.oas.models.media.Schema<>();
        schema.setType("integer");
        schema.setFormat("int32");
        return schema;
    }
    
    private static io.swagger.v3.oas.models.media.Schema<?> createObjectSchema()
    {
        io.swagger.v3.oas.models.media.Schema<?> schema = new io.swagger.v3.oas.models.media.Schema<>();
        schema.setType("object");
        return schema;
    }
    
    private static io.swagger.v3.oas.models.media.Schema<Object> createSuccessResponseSchema()
    {
        io.swagger.v3.oas.models.media.Schema<Object> schema = new io.swagger.v3.oas.models.media.Schema<>();
        schema.setType("object");
        Map<String, io.swagger.v3.oas.models.media.Schema> props = new HashMap<>();
        props.put("result", createStringSchema());
        props.put("message", createStringSchema());
        schema.setProperties(props);
        return schema;
    }
}
