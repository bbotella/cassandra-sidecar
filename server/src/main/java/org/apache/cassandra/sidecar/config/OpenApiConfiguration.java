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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;

/**
 * Configuration for OpenAPI documentation generation
 */
public class OpenApiConfiguration
{
    private static final String API_TITLE = "Cassandra Sidecar API";
    private static final String API_DESCRIPTION = "REST API for managing Apache Cassandra operations";
    private static final String API_VERSION = "1.0.0";
    private static final String LICENSE_NAME = "Apache License 2.0";
    private static final String LICENSE_URL = "https://www.apache.org/licenses/LICENSE-2.0";

    /**
     * Creates OpenAPI configuration with basic information
     *
     * @return configured OpenAPI instance
     */
    public static OpenAPI createOpenApiConfig()
    {
        return new OpenAPI()
               .info(new Info()
                     .title(API_TITLE)
                     .description(API_DESCRIPTION)
                     .version(API_VERSION)
                     .license(new License()
                              .name(LICENSE_NAME)
                              .url(LICENSE_URL)))
               .addServersItem(new Server()
                               .url("http://localhost:9043/api/v1")
                               .description("Development server"));
    }
}