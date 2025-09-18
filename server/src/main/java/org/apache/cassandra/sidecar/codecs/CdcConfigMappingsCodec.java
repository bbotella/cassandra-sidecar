package org.apache.cassandra.sidecar.codecs;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.tasks.CdcConfigRefresherNotifierTask;

/**
 * Message codec for encoding and decoding Cdc Config Mappings change events over the Vert.x event bus.
 */
public class CdcConfigMappingsCodec implements MessageCodec<CdcConfigRefresherNotifierTask.ConfigMappings, CdcConfigRefresherNotifierTask.ConfigMappings>
{
    public static final CdcConfigMappingsCodec INSTANCE = new CdcConfigMappingsCodec();


    public void encodeToWire(Buffer buffer, CdcConfigRefresherNotifierTask.ConfigMappings configMappings)
    {
        CommonCodecs.SHORT.encodeToWire(buffer, (short) configMappings.getCdcConfigMappings().size());
        for (Map.Entry<String, String> entry : configMappings.getCdcConfigMappings().entrySet())
        {
            CommonCodecs.STRING.encodeToWire(buffer, entry.getKey());
            CommonCodecs.STRING.encodeToWire(buffer, entry.getValue());
        }

        CommonCodecs.SHORT.encodeToWire(buffer, (short) configMappings.getKafkaConfigMappings().size());
        for (Map.Entry<String, String> entry : configMappings.getKafkaConfigMappings().entrySet())
        {
            CommonCodecs.STRING.encodeToWire(buffer, entry.getKey());
            CommonCodecs.STRING.encodeToWire(buffer, entry.getValue());
        }

    }

    public CdcConfigRefresherNotifierTask.ConfigMappings decodeFromWire(int pos, Buffer buffer)
    {
        int cdcMapSize = CommonCodecs.SHORT.decodeFromWire(pos, buffer);
        pos += 2;

        Map<String, String> cdcConfigMapping = new HashMap<>();
        for (int i = 0; i < cdcMapSize; i++)
        {
            String key = CommonCodecs.STRING.decodeFromWire(pos, buffer);
            String value = CommonCodecs.STRING.decodeFromWire(pos, buffer);
            cdcConfigMapping.put(key, value);
        }

        Map<String, String> kafkaConfigMapping = new HashMap<>();
        int kafkaMapSize = CommonCodecs.SHORT.decodeFromWire(pos, buffer);
        for (int i = 0; i < kafkaMapSize; i++)
        {
            String key = CommonCodecs.STRING.decodeFromWire(pos, buffer);
            String value = CommonCodecs.STRING.decodeFromWire(pos, buffer);
            kafkaConfigMapping.put(key, value);
        }

        CdcConfigRefresherNotifierTask.ConfigMappings configMappings = new CdcConfigRefresherNotifierTask.ConfigMappings();
        configMappings.setCdcConfigMappings(cdcConfigMapping);
        configMappings.setKafkaConfigMappings(kafkaConfigMapping);
        return configMappings;
    }

    public CdcConfigRefresherNotifierTask.ConfigMappings transform(CdcConfigRefresherNotifierTask.ConfigMappings configMappings)
    {
        return configMappings;
    }

    public String name()
    {
        return "cdc-config-mappings";
    }

    public byte systemCodecID()
    {
        return -1;
    }
}
