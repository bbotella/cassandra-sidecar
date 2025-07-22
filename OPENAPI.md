# OpenAPI Documentation Generation

This project includes Gradle tasks for generating comprehensive OpenAPI documentation for the Cassandra Sidecar APIs.

## Available Tasks

### `generateOpenApiDocs`
Generates OpenAPI documentation files in multiple formats:
- **JSON**: `server/build/docs/openapi/openapi.json` - OpenAPI 3.0 specification in JSON format
- **YAML**: `server/build/docs/openapi/openapi.yaml` - OpenAPI 3.0 specification in YAML format  
- **HTML**: `server/build/docs/openapi/api-docs.html` - Interactive HTML documentation with Swagger UI

```bash
# Generate documentation files
./gradlew generateOpenApiDocs
```

### `openApiDocs`
Generates the documentation and automatically opens the HTML version in your default browser.

```bash
# Generate and open documentation
./gradlew openApiDocs
```

## Generated Documentation Features

The generated documentation includes:

- ✅ **Complete API Coverage** - All endpoints with proper HTTP methods
- ✅ **Interactive UI** - Test APIs directly from the documentation
- ✅ **Response Examples** - Sample JSON responses for each endpoint
- ✅ **Error Documentation** - HTTP status codes and error descriptions
- ✅ **Tag Organization** - Endpoints grouped by functionality (Health, Schema, Snapshots, etc.)
- ✅ **Server Information** - API version, description, and license details

## Runtime API Documentation

In addition to static documentation generation, the running Sidecar server provides:

- **OpenAPI Spec**: `GET http://localhost:9043/api/v1/openapi.json`
- **Interactive Docs**: `GET http://localhost:9043/api/v1/docs`

## Usage Examples

### CI/CD Integration
```bash
# Generate docs as part of build process
./gradlew build generateOpenApiDocs

# Upload generated files to documentation site
cp server/build/docs/openapi/* docs/api/
```

### Development Workflow
```bash
# Quick way to view latest API docs
./gradlew openApiDocs
```

### Integration with External Tools
```bash
# Use generated OpenAPI spec for client generation
swagger-codegen generate -i server/build/docs/openapi/openapi.json -l java -o client/
```

## Output Directory Structure

```
server/build/docs/openapi/
├── openapi.json    # OpenAPI 3.0 specification (JSON)
├── openapi.yaml    # OpenAPI 3.0 specification (YAML)
└── api-docs.html   # Interactive HTML documentation
```

The HTML file is completely self-contained and can be shared or deployed independently.