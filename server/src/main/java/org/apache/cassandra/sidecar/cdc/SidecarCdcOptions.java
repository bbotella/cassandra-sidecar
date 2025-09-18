package org.apache.cassandra.sidecar.cdc;

import java.util.Map;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.ReplicationFactor;

/**
 * Specific sidecar CDC options
 */
public class SidecarCdcOptions implements CdcOptions
{

    private final InstanceMetadataFetcher instanceMetadataFetcher;

    public SidecarCdcOptions(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }


    public ReplicationFactor replicationFactor(String keyspace)
    {

        Map<String, String> replication = instanceMetadataFetcher
                                          .callOnFirstAvailableInstance(instance-> instance.delegate().metadata().getKeyspace(keyspace).getReplication());
        return new ReplicationFactor(replication);
    }

    public String dc()
    {
        return instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings().datacenter());
    }
}
