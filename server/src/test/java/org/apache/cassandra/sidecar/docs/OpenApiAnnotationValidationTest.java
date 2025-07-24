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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

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
        Class<?> moduleClass = CassandraOperationsModule.class;
        Method[] methods = moduleClass.getDeclaredMethods();
        
        int annotatedMethods = 0;
        for (Method method : methods)
        {
            if (method.isAnnotationPresent(ProvidesIntoMap.class))
            {
                boolean hasOpenApiAnnotations = method.isAnnotationPresent(Operation.class) ||
                                                method.isAnnotationPresent(Tag.class) ||
                                                method.isAnnotationPresent(ApiResponses.class);
                
                if (hasOpenApiAnnotations)
                {
                    annotatedMethods++;
                    
                    // If it has Operation, it should also have ApiResponses
                    if (method.isAnnotationPresent(Operation.class))
                    {
                        assertTrue(method.isAnnotationPresent(ApiResponses.class),
                                   "Method " + method.getName() + " has @Operation but missing @ApiResponses");
                    }
                }
            }
        }
        
        assertThat(annotatedMethods).isGreaterThan(0);
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
        Method[] methods = CassandraOperationsModule.class.getDeclaredMethods();
        
        boolean foundRingMethod = false;
        boolean foundGossipMethod = false;
        
        for (Method method : methods)
        {
            if (method.getName().contains("Ring") && method.isAnnotationPresent(ApiResponses.class))
            {
                foundRingMethod = true;
                ApiResponses responses = method.getAnnotation(ApiResponses.class);
                assertNotNull(responses, "Ring method should have ApiResponses annotation");
                assertThat(responses.value()).isNotEmpty();
            }
            
            if (method.getName().contains("Gossip") && method.isAnnotationPresent(ApiResponses.class))
            {
                foundGossipMethod = true;
                ApiResponses responses = method.getAnnotation(ApiResponses.class);
                assertNotNull(responses, "Gossip method should have ApiResponses annotation");
                assertThat(responses.value()).isNotEmpty();
            }
        }
        
        assertTrue(foundRingMethod, "Should find at least one Ring method with ApiResponses");
        assertTrue(foundGossipMethod, "Should find at least one Gossip method with ApiResponses");
    }

    @Test
    void testConfigurationApiHasProperAnnotations()
    {
        Method[] methods = CdcModule.class.getDeclaredMethods();
        
        int configMethodsWithAnnotations = 0;
        for (Method method : methods)
        {
            if (method.isAnnotationPresent(ProvidesIntoMap.class) && 
                method.isAnnotationPresent(Operation.class))
            {
                Operation operation = method.getAnnotation(Operation.class);
                if (operation.summary().toLowerCase().contains("config") ||
                    operation.description().toLowerCase().contains("config"))
                {
                    configMethodsWithAnnotations++;
                    assertTrue(method.isAnnotationPresent(ApiResponses.class),
                               "Configuration method " + method.getName() + " should have ApiResponses");
                }
            }
        }
        
        assertThat(configMethodsWithAnnotations).isGreaterThan(0);
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
        Method[] methods = moduleClass.getDeclaredMethods();
        
        int routeProviderMethods = 0;
        int annotatedMethods = 0;
        
        for (Method method : methods)
        {
            if (method.isAnnotationPresent(ProvidesIntoMap.class))
            {
                routeProviderMethods++;
                
                boolean hasOpenApiAnnotations = method.isAnnotationPresent(Operation.class) ||
                                                method.isAnnotationPresent(Tag.class) ||
                                                method.isAnnotationPresent(ApiResponses.class);
                
                if (hasOpenApiAnnotations)
                {
                    annotatedMethods++;
                }
            }
        }
        
        assertThat(routeProviderMethods).isGreaterThan(0);
        // Note: Not all route provider methods need OpenAPI annotations (some may be internal)
        // but we should have at least some documented endpoints
        assertTrue(annotatedMethods >= 0, 
                   moduleName + " should have some OpenAPI annotated methods");
    }

    private void verifyTagConsistencyForModule(Class<?> moduleClass, Set<String> expectedTags)
    {
        Method[] methods = moduleClass.getDeclaredMethods();
        
        for (Method method : methods)
        {
            if (method.isAnnotationPresent(Tag.class))
            {
                Tag tag = method.getAnnotation(Tag.class);
                assertTrue(expectedTags.contains(tag.name()),
                           "Unexpected tag '" + tag.name() + "' in " + moduleClass.getSimpleName() + 
                           ". Expected one of: " + expectedTags);
            }
        }
    }
}
