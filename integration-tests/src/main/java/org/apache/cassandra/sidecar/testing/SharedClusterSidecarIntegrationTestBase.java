package org.apache.cassandra.sidecar.testing;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import static io.vertx.core.Vertx.vertx;
import static org.apache.cassandra.sidecar.testing.MtlsTestHelper.EMPTY_PASSWORD_STRING;

public class SharedClusterSidecarIntegrationTestBase extends SharedClusterIntegrationTestBase
{

    public static final String SIDECAR_HOST = "localhost";

    private WebClient trustedClient;
    private WebClient noAuthClient;

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }

    /**
     * Returns the request with the configured trusted client to the provided URI.
     * Defaults to GET requests for {@code 127.0.0.1}.
     *
     * @param requestURI the URI for the GET request
     * @return the request for sidecar using the trusted client
     */
    protected HttpRequest<Buffer> sidecarRequestWithTrustedClient(String requestURI, int port)
    {
        return sidecarRequestWithTrustedClient(HttpMethod.GET, SIDECAR_HOST, requestURI, port);
    }

    /**
     * Returns the request with the configured trusted client to the provided host and URI.
     *
     * @param method     the HTTP method
     * @param host       the sidecar host
     * @param requestURI the URI for the GET request
     * @return the request for sidecar using the trusted client
     */
    protected HttpRequest<Buffer> sidecarRequestWithTrustedClient(HttpMethod method, String host, String requestURI, int port)
    {
        WebClient client = trustedClient();
        return client.request(method, port, host, requestURI);
    }

    /**
     * @return a client that configures the truststore and the client keystore
     */
    public WebClient trustedClient()
    {
        if (trustedClient != null)
        {
            return trustedClient;
        }

        WebClientOptions clientOptions = new WebClientOptions()
                .setKeyStoreOptions(new JksOptions()
                        .setPath(mtlsTestHelper.clientKeyStorePath.toString())
                        .setPassword(EMPTY_PASSWORD_STRING))
                .setTrustStoreOptions(new JksOptions()
                        .setPath(mtlsTestHelper.trustStorePath())
                        .setPassword(EMPTY_PASSWORD_STRING))
                .setSsl(true);
        trustedClient = WebClient.create(vertx(), clientOptions);
        return trustedClient;
    }

    /**
     * @return a client that configures the truststore, but does not provide a client identity
     */
    public WebClient noAuthClient()
    {
        if (noAuthClient != null)
        {
            return noAuthClient;
        }

        WebClientOptions clientOptions = new WebClientOptions()
                .setTrustStoreOptions(new JksOptions()
                        .setPath(mtlsTestHelper.trustStorePath())
                        .setPassword(mtlsTestHelper.trustStorePassword()))
                .setSsl(true);
        noAuthClient = WebClient.create(vertx(), clientOptions);
        return noAuthClient;
    }
}
