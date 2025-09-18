package org.apache.cassandra.sidecar.cdc;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.kafka.AvroGenericRecordSerializer;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Taking a schema, this class serializes a CDC into AVRO format
 */
public class CdcAvroSerializer extends AvroGenericRecordSerializer
{
    public CdcAvroSerializer(SchemaStore schemaStore,
                             InstanceMetadataFetcher instanceMetadataFetcher,
                             CassandraBridgeFactory cassandraBridgeFactory)
    {
        super(schemaStore, key ->
                           TypeCache.get(cassandraBridgeFactory
                                         .get(instanceMetadataFetcher.callOnFirstAvailableInstance(instance->
                                                                                                   instance.delegate().nodeSettings()).releaseVersion()).getVersion())
                                    .getType(key.keyspace, key.type), "org.apache.cassandra");
    }
}
