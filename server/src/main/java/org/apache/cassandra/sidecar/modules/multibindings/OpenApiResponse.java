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

package org.apache.cassandra.sidecar.modules.multibindings;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Custom annotation to define OpenAPI response configurations for route key interfaces.
 * This annotation is repeatable to allow multiple response configurations per endpoint.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(OpenApiResponses.class)
public @interface OpenApiResponse
{
    /**
     * HTTP response code (e.g., "200", "404", "500")
     */
    String responseCode();
    
    /**
     * Description of the response
     */
    String description();
    
    /**
     * Media type of the response (e.g., "application/json", "application/octet-stream")
     */
    String mediaType() default "";
    
    /**
     * Schema class for the response body
     */
    Class<?> schemaClass() default Void.class;
    
    /**
     * Example response body as JSON string
     */
    String example() default "";
    
    /**
     * Reference to a schema component (e.g., "#/components/schemas/ResponseType")
     */
    String schemaRef() default "";
    
    /**
     * OpenAPI schema type (e.g., "object", "array", "string")
     */
    String schemaType() default "";
}
