package org.apache.cassandra.sidecar.codecs;

import io.vertx.core.eventbus.impl.codecs.BooleanMessageCodec;
import io.vertx.core.eventbus.impl.codecs.ByteArrayMessageCodec;
import io.vertx.core.eventbus.impl.codecs.IntMessageCodec;
import io.vertx.core.eventbus.impl.codecs.ShortMessageCodec;
import io.vertx.core.eventbus.impl.codecs.StringMessageCodec;

/**
 * Common message codecs for primitive types used across the event bus.
 */
public class CommonCodecs
{
    /** String message codec instance. */
    public static final StringMessageCodec STRING = new StringMessageCodec();
    /** Short message codec instance. */
    public static final ShortMessageCodec SHORT = new ShortMessageCodec();
    /** Byte array message codec instance. */
    public static final ByteArrayMessageCodec BYTE_ARRAY = new ByteArrayMessageCodec();
    /** Integer message codec instance. */
    public static final IntMessageCodec INT = new IntMessageCodec();
    /** Boolean message codec instance. */
    public static final BooleanMessageCodec BOOL = new BooleanMessageCodec();
}
