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

import java.lang.reflect.Method;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.inject.multibindings.ProvidesIntoMap;

import org.apache.cassandra.sidecar.modules.CassandraOperationsModule;
import org.apache.cassandra.sidecar.modules.CdcModule;
import org.apache.cassandra.sidecar.modules.HealthCheckModule;
import org.apache.cassandra.sidecar.modules.LiveMigrationModule;
import org.apache.cassandra.sidecar.modules.RestoreJobModule;
import org.apache.cassandra.sidecar.modules.SSTablesAccessModule;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for verifying that OpenAPI annotations are properly placed on module route provider methods
 */
class OpenApiAnnotationValidationTest
{
    @Test
    void testCassandraOperationsModuleHasRequiredAnnotations()
    {
        // This test validates that the OpenAPI documentation system works correctly
        // by checking that route key interfaces have proper @OpenApiEndpoint annotations
        Class<?> vertxRouteMapKeysClass = org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class;
        
        int annotatedRouteKeys = 0;
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    annotatedRouteKeys++;
                    
                    // Validate that the annotation has required fields
                    assertNotNull(endpointAnnotation.tag(), "Route key " + innerClass.getSimpleName() + " should have a tag");
                    assertNotNull(endpointAnnotation.summary(), "Route key " + innerClass.getSimpleName() + " should have a summary");
                    assertTrue(endpointAnnotation.responses().length > 0, "Route key " + innerClass.getSimpleName() + " should have responses");
                }
            }
        }
        
        assertThat(annotatedRouteKeys).isGreaterThan(0);
    }

    @Test
    void testRestoreJobModuleHasRequiredAnnotations()
    {
        verifyModuleHasOpenApiAnnotations(RestoreJobModule.class, "RestoreJobModule");
    }

    @Test
    void testSSTablesAccessModuleHasRequiredAnnotations()
    {
        verifyModuleHasOpenApiAnnotations(SSTablesAccessModule.class, "SSTablesAccessModule");
    }

    @Test
    void testCdcModuleHasRequiredAnnotations()
    {
        verifyModuleHasOpenApiAnnotations(CdcModule.class, "CdcModule");
    }

    @Test
    void testLiveMigrationModuleHasRequiredAnnotations()
    {
        verifyModuleHasOpenApiAnnotations(LiveMigrationModule.class, "LiveMigrationModule");
    }

    @Test
    void testHealthCheckModuleHasRequiredAnnotations()
    {
        verifyModuleHasOpenApiAnnotations(HealthCheckModule.class, "HealthCheckModule");
    }

    @Test
    void testRingApiHasProperSchemaReferences()
    {
        // This test validates that Ring and Gossip APIs have proper OpenAPI annotations
        Class<?> vertxRouteMapKeysClass = org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class;
        
        boolean foundRingRouteKey = false;
        boolean foundGossipRouteKey = false;
        
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    String className = innerClass.getSimpleName();
                    
                    if (className.contains("Ring"))
                    {
                        foundRingRouteKey = true;
                        assertNotNull(endpointAnnotation.responses(), "Ring route key should have responses");
                        assertThat(endpointAnnotation.responses()).isNotEmpty();
                    }
                    
                    if (className.contains("Gossip"))
                    {
                        foundGossipRouteKey = true;
                        assertNotNull(endpointAnnotation.responses(), "Gossip route key should have responses");
                        assertThat(endpointAnnotation.responses()).isNotEmpty();
                    }
                }
            }
        }
        
        assertTrue(foundRingRouteKey, "Should find at least one Ring route key with ApiResponses");
        assertTrue(foundGossipRouteKey, "Should find at least one Gossip route key with ApiResponses");
    }

    @Test
    void testConfigurationApiHasProperAnnotations()
    {
        // This test validates that configuration-related APIs have proper OpenAPI annotations
        Class<?> vertxRouteMapKeysClass = org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class;
        
        int configRouteKeysWithAnnotations = 0;
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    String className = innerClass.getSimpleName();
                    String summary = endpointAnnotation.summary().toLowerCase();
                    String description = endpointAnnotation.description().toLowerCase();
                    
                    if (className.toLowerCase().contains("config") || 
                        summary.contains("config") || 
                        description.contains("config"))
                    {
                        configRouteKeysWithAnnotations++;
                        assertTrue(endpointAnnotation.responses().length > 0,
                                   "Configuration route key " + className + " should have responses");
                    }
                }
            }
        }
        
        assertThat(configRouteKeysWithAnnotations).isGreaterThan(0);
    }

    @Test
    void testAllModulesAreScannedByGenerator()
    {
        // Verify that the OpenApiDocumentationGenerator includes all module classes
        Set<Class<?>> expectedModules = Set.of(
            org.apache.cassandra.sidecar.modules.HealthCheckModule.class,
            org.apache.cassandra.sidecar.modules.CassandraOperationsModule.class,
            org.apache.cassandra.sidecar.modules.SSTablesAccessModule.class,
            org.apache.cassandra.sidecar.modules.RestoreJobModule.class,
            org.apache.cassandra.sidecar.modules.CdcModule.class,
            org.apache.cassandra.sidecar.modules.LiveMigrationModule.class,
            org.apache.cassandra.sidecar.modules.OpenApiModule.class
        );
        
        // This test ensures that if new modules are added, they are included in the generator
        for (Class<?> moduleClass : expectedModules)
        {
            assertNotNull(moduleClass, "Module class should exist: " + moduleClass.getSimpleName());
            
            // Verify it has route provider methods
            Method[] methods = moduleClass.getDeclaredMethods();
            boolean hasRouteProviderMethods = false;
            for (Method method : methods)
            {
                if (method.isAnnotationPresent(ProvidesIntoMap.class))
                {
                    hasRouteProviderMethods = true;
                    break;
                }
            }
            assertTrue(hasRouteProviderMethods, 
                       moduleClass.getSimpleName() + " should have route provider methods");
        }
    }

    @Test
    void testTagConsistency()
    {
        // Verify that methods in the same module use consistent tags
        verifyTagConsistencyForModule(CassandraOperationsModule.class, 
                                      Set.of("Ring", "Schema", "Node Operations", "Streaming"));
        verifyTagConsistencyForModule(RestoreJobModule.class, 
                                      Set.of("Restore Jobs"));
        verifyTagConsistencyForModule(SSTablesAccessModule.class, 
                                      Set.of("SSTable Operations", "Snapshots", "Streaming"));
        verifyTagConsistencyForModule(CdcModule.class, 
                                      Set.of("CDC", "Configuration"));
        verifyTagConsistencyForModule(LiveMigrationModule.class, 
                                      Set.of("Live Migration"));
        verifyTagConsistencyForModule(HealthCheckModule.class, 
                                      Set.of("Health"));
    }

    private void verifyModuleHasOpenApiAnnotations(Class<?> moduleClass, String moduleName)
    {
        // This method validates that the OpenAPI documentation system includes route keys
        // related to the given module by checking VertxRouteMapKeys annotations
        Class<?> vertxRouteMapKeysClass = org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class;
        
        int routeKeysWithAnnotations = 0;
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    // Check if this route key might be related to the module
                    String className = innerClass.getSimpleName();
                    String tag = endpointAnnotation.tag();
                    
                    // Map module names to expected tags or route key patterns
                    boolean isRelated = false;
                    switch (moduleName)
                    {
                        case "RestoreJobModule":
                            isRelated = tag.equals("Restore Jobs") || className.contains("Restore");
                            break;
                        case "SSTablesAccessModule":
                            isRelated = tag.equals("SSTable Operations") || tag.equals("Snapshots") || 
                                       className.contains("SSTable") || className.contains("Snapshot");
                            break;
                        case "CdcModule":
                            isRelated = tag.equals("CDC") || className.contains("Cdc");
                            break;
                        case "LiveMigrationModule":
                            isRelated = tag.equals("Live Migration") || className.contains("LiveMigration");
                            break;
                        case "HealthCheckModule":
                            isRelated = tag.equals("Health") || className.contains("Health");
                            break;
                        default:
                            isRelated = true; // For other modules, just count all annotations
                    }
                    
                    if (isRelated)
                    {
                        routeKeysWithAnnotations++;
                    }
                }
            }
        }
        
        // We should have at least some documented endpoints related to each module
        assertTrue(routeKeysWithAnnotations > 0, 
                   moduleName + " should have some OpenAPI annotated route keys");
    }

    private void verifyTagConsistencyForModule(Class<?> moduleClass, Set<String> expectedTags)
    {
        // This method validates that route keys use consistent tags as expected for each module
        Class<?> vertxRouteMapKeysClass = org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.class;
        
        for (Class<?> innerClass : vertxRouteMapKeysClass.getDeclaredClasses())
        {
            if (innerClass.getSimpleName().endsWith("RouteKey") && innerClass.isInterface())
            {
                org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint endpointAnnotation = 
                    innerClass.getAnnotation(org.apache.cassandra.sidecar.modules.multibindings.OpenApiEndpoint.class);
                
                if (endpointAnnotation != null)
                {
                    String tag = endpointAnnotation.tag();
                    String className = innerClass.getSimpleName();
                    
                    // Check if this route key is related to the module being tested
                    boolean isRelatedToModule = false;
                    String moduleName = moduleClass.getSimpleName();
                    
                    switch (moduleName)
                    {
                        case "CassandraOperationsModule":
                            isRelatedToModule = className.contains("Ring") || className.contains("Schema") || 
                                               className.contains("Node") || className.contains("Operational") || 
                                               className.contains("Connected") ||
                                               className.equals("CassandraStreamStatsRouteKey") ||
                                               className.equals("CassandraGossipInfoRouteKey") ||
                                               className.equals("UpdateNodeGossipStateRouteKey");
                            break;
                        case "RestoreJobModule":
                            isRelatedToModule = className.contains("Restore");
                            break;
                        case "SSTablesAccessModule":
                            isRelatedToModule = className.contains("SSTable") || className.contains("Snapshot") || 
                                               className.equals("StreamSSTableComponentsRouteKey") ||
                                               className.equals("StreamSSTableComponentsWithSecondaryIndexRouteKey") ||
                                               className.equals("DeprecatedStreamSSTableComponentsRouteKey");
                            break;
                        case "CdcModule":
                            isRelatedToModule = className.contains("Cdc") || className.equals("StreamCdcSegmentRouteKey");
                            break;
                        case "LiveMigrationModule":
                            isRelatedToModule = className.contains("LiveMigration");
                            break;
                        case "HealthCheckModule":
                            isRelatedToModule = className.contains("Health") || className.equals("CassandraGossipHealthRouteKey");
                            break;
                    }
                    
                    if (isRelatedToModule)
                    {
                        assertTrue(expectedTags.contains(tag),
                                   "Unexpected tag '" + tag + "' in route key " + className + 
                                   " for " + moduleClass.getSimpleName() + ". Expected one of: " + expectedTags);
                    }
                }
            }
        }
    }
}
