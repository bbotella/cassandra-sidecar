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

package org.apache.cassandra.sidecar.config.yaml;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.OpenApiConfiguration;

/**
 * Implementation of {@link OpenApiConfiguration}
 */
public class OpenApiConfigurationImpl implements OpenApiConfiguration
{
    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_TITLE = "Cassandra Sidecar API";
    public static final String DEFAULT_DESCRIPTION = "REST API for managing Apache Cassandra operations";
    public static final String DEFAULT_VERSION = "1.0.0";
    public static final String DEFAULT_LICENSE_NAME = "Apache License 2.0";
    public static final String DEFAULT_LICENSE_URL = "https://www.apache.org/licenses/LICENSE-2.0";
    public static final String DEFAULT_SERVER_URL = "http://localhost:9043/api/v1";
    public static final String DEFAULT_SERVER_DESCRIPTION = "Development server";

    @JsonProperty(value = "enabled", defaultValue = "true")
    protected final boolean enabled;

    @JsonProperty(value = "title", defaultValue = DEFAULT_TITLE)
    protected final String title;

    @JsonProperty(value = "description", defaultValue = DEFAULT_DESCRIPTION)
    protected final String description;

    @JsonProperty(value = "version", defaultValue = DEFAULT_VERSION)
    protected final String version;

    @JsonProperty(value = "license_name", defaultValue = DEFAULT_LICENSE_NAME)
    protected final String licenseName;

    @JsonProperty(value = "license_url", defaultValue = DEFAULT_LICENSE_URL)
    protected final String licenseUrl;

    @JsonProperty(value = "server_url", defaultValue = DEFAULT_SERVER_URL)
    protected final String serverUrl;

    @JsonProperty(value = "server_description", defaultValue = DEFAULT_SERVER_DESCRIPTION)
    protected final String serverDescription;

    public OpenApiConfigurationImpl()
    {
        this(DEFAULT_ENABLED, DEFAULT_TITLE, DEFAULT_DESCRIPTION, DEFAULT_VERSION,
             DEFAULT_LICENSE_NAME, DEFAULT_LICENSE_URL, DEFAULT_SERVER_URL, DEFAULT_SERVER_DESCRIPTION);
    }

    public OpenApiConfigurationImpl(boolean enabled,
                                    String title,
                                    String description,
                                    String version,
                                    String licenseName,
                                    String licenseUrl,
                                    String serverUrl,
                                    String serverDescription)
    {
        this.enabled = enabled;
        this.title = title;
        this.description = description;
        this.version = version;
        this.licenseName = licenseName;
        this.licenseUrl = licenseUrl;
        this.serverUrl = serverUrl;
        this.serverDescription = serverDescription;
    }

    @Override
    public boolean enabled()
    {
        return enabled;
    }

    @Override
    public String title()
    {
        return title;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public String version()
    {
        return version;
    }

    @Override
    public String licenseName()
    {
        return licenseName;
    }

    @Override
    public String licenseUrl()
    {
        return licenseUrl;
    }

    @Override
    public String serverUrl()
    {
        return serverUrl;
    }

    @Override
    public String serverDescription()
    {
        return serverDescription;
    }

    /**
     * Builder class for {@link OpenApiConfigurationImpl}
     */
    public static class Builder
    {
        private boolean enabled = DEFAULT_ENABLED;
        private String title = DEFAULT_TITLE;
        private String description = DEFAULT_DESCRIPTION;
        private String version = DEFAULT_VERSION;
        private String licenseName = DEFAULT_LICENSE_NAME;
        private String licenseUrl = DEFAULT_LICENSE_URL;
        private String serverUrl = DEFAULT_SERVER_URL;
        private String serverDescription = DEFAULT_SERVER_DESCRIPTION;

        public Builder enabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }

        public Builder title(String title)
        {
            this.title = title;
            return this;
        }

        public Builder description(String description)
        {
            this.description = description;
            return this;
        }

        public Builder version(String version)
        {
            this.version = version;
            return this;
        }

        public Builder licenseName(String licenseName)
        {
            this.licenseName = licenseName;
            return this;
        }

        public Builder licenseUrl(String licenseUrl)
        {
            this.licenseUrl = licenseUrl;
            return this;
        }

        public Builder serverUrl(String serverUrl)
        {
            this.serverUrl = serverUrl;
            return this;
        }

        public Builder serverDescription(String serverDescription)
        {
            this.serverDescription = serverDescription;
            return this;
        }

        public OpenApiConfigurationImpl build()
        {
            return new OpenApiConfigurationImpl(enabled, title, description, version,
                                                licenseName, licenseUrl, serverUrl, serverDescription);
        }
    }
}
