package org.apache.cassandra.sidecar.cdc;

import java.util.function.Consumer;

import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.kafka.KafkaPublisher;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation class for a Cdc Event Consumer using Kafka
 */
public class CdcEventConsumer implements EventConsumer
{
    private final transient KafkaPublisher kafka;

    public CdcEventConsumer(KafkaPublisher kafka)
    {
        this.kafka = kafka;
    }

    public void accept(CdcEvent cdcEvent)
    {
        kafka.processEvent(cdcEvent);
    }

    public @NotNull Consumer<CdcEvent> andThen(@NotNull Consumer<? super CdcEvent> after)
    {
        return EventConsumer.super.andThen(after);
    }
}
