package cassandra.sidecar.routes;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutesIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    @Test
    void healthHappyPathTest() throws Exception
    {
        HttpResponse<Buffer> response = sidecarRequestWithTrustedClient("/api/v1/__health", server.actualPort())
                .send()
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.SECONDS);
        assertEquals("OK", response.bodyAsJsonObject().getString("status"));
    }
}
