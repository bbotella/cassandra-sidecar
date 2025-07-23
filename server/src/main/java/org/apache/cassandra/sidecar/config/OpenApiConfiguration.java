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

package org.apache.cassandra.sidecar.config;

/**
 * Configuration for OpenAPI documentation
 */
public interface OpenApiConfiguration
{
    /**
     * @return whether OpenAPI documentation is enabled
     */
    boolean enabled();

    /**
     * @return the title for the API documentation
     */
    String title();

    /**
     * @return the description for the API documentation
     */
    String description();

    /**
     * @return the version of the API
     */
    String version();

    /**
     * @return the license name
     */
    String licenseName();

    /**
     * @return the license URL
     */
    String licenseUrl();

    /**
     * @return the server URL for the API
     */
    String serverUrl();

    /**
     * @return the server description
     */
    String serverDescription();
}
