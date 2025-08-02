<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# OpenAPI Documentation

This project uses standard JAX-RS and MicroProfile OpenAPI annotations to generate comprehensive API documentation for the Cassandra Sidecar.

## Architecture Overview

### Standard Annotations Approach
The Cassandra Sidecar uses industry-standard annotations for OpenAPI documentation generation:

- **JAX-RS Annotations**: `@GET`, `@POST`, `@PUT`, `@DELETE`, `@Path` - Define HTTP methods and routes
- **MicroProfile OpenAPI Annotations**: `@Operation`, `@APIResponse`, `@Schema` - Add comprehensive API documentation
- **SmallRye OpenAPI Plugin**: Gradle plugin that processes annotations to generate OpenAPI specifications

### Benefits
- ✅ **Standards Compliance** - Uses widely adopted JAX-RS and MicroProfile specifications
- ✅ **Tool Integration** - Works with existing OpenAPI ecosystem and code generators
- ✅ **Maintainability** - Documentation lives alongside code, reducing maintenance overhead
- ✅ **Rich Schema Support** - Specific response types with examples instead of generic objects

## Generating Documentation

### Build-time Generation
```bash
# Generate OpenAPI specifications (JSON and YAML)
./gradlew generateOpenApiSpec

# Generated files:
# - server/build/generated/openapi/openapi.json
# - server/build/generated/openapi/openapi.yaml
```

### Integration with Build
The generated OpenAPI specifications are automatically copied to the application resources during build:
```bash
./gradlew build
# OpenAPI specs are packaged in: server/build/resources/main/openapi/
```

## Runtime API Endpoints

The running Sidecar server provides multiple OpenAPI endpoints:

### OpenAPI Specifications
- **JSON Format**: `GET http://localhost:9043/api/v1/openapi.json`
- **YAML Format**: `GET http://localhost:9043/api/v1/openapi.yaml`

### Interactive Documentation
- **Swagger UI**: `GET http://localhost:9043/api/v1/docs`

## Documentation Features

### Comprehensive Coverage
- **All Endpoints** - Health checks, schema operations, snapshots, restore jobs, live migration, CDC, etc.
- **HTTP Methods** - GET, POST, PUT, DELETE, PATCH operations properly documented
- **Path Parameters** - Keyspace, table, snapshot names with validation patterns
- **Response Schemas** - Specific typed responses instead of generic objects

### Rich Schema Information
- **Response Classes** - `HealthResponse`, `SchemaResponse`, `TableStatsResponse`, etc.
- **Example Values** - JSON examples showing expected response formats
- **Error Responses** - HTTP status codes with detailed error descriptions
- **Content Types** - Proper media type specifications (JSON, YAML, HTML, binary)

### Organized Structure
- **Tags** - Endpoints grouped by functionality (Health, Cassandra Operations, Schema, etc.)
- **Deprecation Markers** - Legacy endpoints clearly marked as deprecated
- **Descriptions** - Clear summaries and detailed descriptions for each endpoint

## Adding Documentation to New Endpoints

When creating new API endpoints, add the appropriate annotations:

```java
@GET
@Path("/api/v1/new-endpoint")
@Operation(summary = "Brief description", 
           description = "Detailed description of what this endpoint does")
@APIResponse(description = "Success response description",
             responseCode = "200",
             content = @Content(mediaType = "application/json",
             schema = @Schema(implementation = MyResponseClass.class)))
public VertxRoute myEndpoint(RouteBuilder.Factory factory, MyHandler handler) {
    return factory.builderForRoute()
                  .handler(handler)
                  .build();
}
```

## Client Code Generation

Use the generated OpenAPI specifications with standard tools:

```bash
# Generate Java client
swagger-codegen generate -i server/build/generated/openapi/openapi.json -l java -o client/

# Generate Python client  
swagger-codegen generate -i server/build/generated/openapi/openapi.yaml -l python -o python-client/

# Generate TypeScript client
swagger-codegen generate -i server/build/generated/openapi/openapi.json -l typescript-fetch -o ts-client/
```

## Development Workflow

### Local Development
```bash
# Start the server
./gradlew run

# Access interactive documentation
open http://localhost:9043/api/v1/docs

# Get OpenAPI spec for external tools
curl http://localhost:9043/api/v1/openapi.json
curl http://localhost:9043/api/v1/openapi.yaml
```

### CI/CD Integration
```bash
# Generate specs as part of build
./gradlew generateOpenApiSpec

# Copy specs for documentation deployment
cp server/build/generated/openapi/* docs/api/

# Validate API changes
swagger-codegen validate -i server/build/generated/openapi/openapi.json
```

## Migration Notes

This documentation system replaces the previous custom `@OpenApiEndpoint` annotation approach with standards-compliant JAX-RS and MicroProfile annotations. The migration:

- **Removed** 1,343-line custom `OpenApiDocumentationGenerator.java`
- **Eliminated** custom annotation processing infrastructure  
- **Added** comprehensive standard annotations across all modules
- **Enhanced** response schemas with specific types and examples
- **Integrated** with Gradle SmallRye OpenAPI plugin for automatic generation

## Output Structure

```
server/build/generated/openapi/
├── openapi.json    # OpenAPI 3.1.0 specification (JSON)
└── openapi.yaml    # OpenAPI 3.1.0 specification (YAML)

server/build/resources/main/openapi/  # Packaged in JAR
├── openapi.json    # Runtime-accessible JSON spec  
└── openapi.yaml    # Runtime-accessible YAML spec
```

The specifications are fully compliant with OpenAPI 3.1.0 and include comprehensive schemas, examples, and documentation for all Cassandra Sidecar APIs.