package org.apache.cassandra.sidecar.cdc;


import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;

/**
 * Class to convert CQL schemas into Avro schemas
 */
public class SidecarCqlToAvroSchemaConverter implements CqlToAvroSchemaConverter
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final CassandraBridgeFactory cassandraBridgeFactory;


    @Inject
    public SidecarCqlToAvroSchemaConverter(InstanceMetadataFetcher instanceMetadataFetcher,
                                           CassandraBridgeFactory cassandraBridgeFactory)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.cassandraBridgeFactory = cassandraBridgeFactory;
    }

    public CassandraBridge cassandraBridge()
    {
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        return cassandraBridgeFactory.get(nodeSettings.releaseVersion());
    }


    public Schema convert(CqlTable cqlTable)
    {
        return CdcBridgeFactory.getCqlToAvroSchemaConverter(cassandraBridge()).convert(cqlTable);
    }
}
