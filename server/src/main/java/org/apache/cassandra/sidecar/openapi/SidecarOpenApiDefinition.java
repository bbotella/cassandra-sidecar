package org.apache.cassandra.sidecar.openapi;

import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.info.Contact;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.info.License;
import org.eclipse.microprofile.openapi.annotations.servers.Server;

@OpenAPIDefinition(
    info = @Info(
        title = "Apache Cassandra Sidecar API",
        version = "1.0.0", // Replace with actual or dynamic version if possible later
        description = "RESTful API to manage and interact with Apache Cassandra nodes.",
        license = @License(
            name = "Apache 2.0",
            url = "http://www.apache.org/licenses/LICENSE-2.0.html"),
        contact = @Contact(
            name = "Apache Cassandra",
            url = "https://cassandra.apache.org")
    ),
    servers = {
        @Server(url = "http://localhost:9043", description = "Default Sidecar server URL")
        // Add other common server URLs if applicable, or this can be configured/overridden
    }
)
public class SidecarOpenApiDefinition {
    // This class is purely for holding the @OpenAPIDefinition annotation.
    // It does not need any methods or fields.
}
