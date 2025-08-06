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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.v3.oas.annotations.Hidden;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/**
 * Handler that serves the Swagger UI for API documentation
 */
@Hidden
@Singleton
public class OpenApiUIHandler implements Handler<RoutingContext>
{
    private static final String SWAGGER_UI_HTML = 
        "<!DOCTYPE html>\n" +
        "<html lang=\"en\">\n" +
        "<head>\n" +
        "    <meta charset=\"UTF-8\">\n" +
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
        "    </style>\n" +
        "</head>\n" +
        "<body>\n" +
        "    <div id=\"swagger-ui\"></div>\n" +
        "    <script src=\"https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js\"></script>\n" +
        "    <script src=\"https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-standalone-preset.js\"></script>\n" +
        "    <script>\n" +
        "        window.onload = function() {\n" +
        "            const ui = SwaggerUIBundle({\n" +
        "                url: '/openapi.json',\n" +
        "                dom_id: '#swagger-ui',\n" +
        "                deepLinking: true,\n" +
        "                presets: [\n" +
        "                    SwaggerUIBundle.presets.apis,\n" +
        "                    SwaggerUIStandalonePreset\n" +
        "                ],\n" +
        "                plugins: [\n" +
        "                    SwaggerUIBundle.plugins.DownloadUrl\n" +
        "                ],\n" +
        "                layout: \"StandaloneLayout\"\n" +
        "            });\n" +
        "        };\n" +
        "    </script>\n" +
        "</body>\n" +
        "</html>";

    @Inject
    public OpenApiUIHandler()
    {
    }

    @Override
    public void handle(RoutingContext context)
    {
        context.response()
               .putHeader("Content-Type", "text/html; charset=utf-8")
               .end(SWAGGER_UI_HTML);
    }
}
