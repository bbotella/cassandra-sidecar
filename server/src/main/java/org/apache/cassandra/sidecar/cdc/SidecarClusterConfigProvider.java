package org.apache.cassandra.sidecar.cdc;

import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Host;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * Provides cluster configuration information for CDC (Change Data Capture) operations by
 * interfacing with Cassandra sidecar instances.
 *
 * <p>This implementation of {@link ClusterConfigProvider} retrieves cluster topology and
 * configuration details from available Cassandra instances through the sidecar's metadata
 * fetcher. It serves as a bridge between the CDC framework and the sidecar's knowledge
 * of the Cassandra cluster.
 *
 */
public class SidecarClusterConfigProvider implements ClusterConfigProvider
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;

    public SidecarClusterConfigProvider(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    public String dc()
    {
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        return nodeSettings.datacenter();
    }

    public Set<CassandraInstance> getCluster()
    {
        Set<Host> hosts = instanceMetadataFetcher.callOnFirstAvailableInstance(instance ->
                                                                               instance.delegate().metadata().getAllHosts());
        return hosts.stream()
                    .filter(host -> host.getListenAddress() != null)
                    .flatMap(host -> host.getTokens().stream()
                                         .map(token -> new CassandraInstance(
                                         token.toString(),
                                         host.getEndPoint().resolve().getHostName(),
                                         host.getDatacenter()
                                         ))
                    ).collect(Collectors.toSet());
    }

    public Partitioner partitioner()
    {
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        String[] parts = nodeSettings.partitioner().split("\\.");
        return Partitioner.valueOf(parts[parts.length - 1]);
    }
}
