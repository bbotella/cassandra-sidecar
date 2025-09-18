package org.apache.cassandra.sidecar.codecs;

import java.math.BigInteger;

import org.apache.commons.lang3.mutable.MutableInt;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

/**
 * Message codec for encoding and decoding BigInteger values over the Vert.x event bus.
 */
public class BigIntegerCodec implements MessageCodec<BigInteger, BigInteger>
{
    public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

    /**
     * Encodes a BigInteger to the wire buffer.
     */
    public void encodeToWire(Buffer buf, BigInteger bigInteger)
    {
        CommonCodecs.BYTE_ARRAY.encodeToWire(buf, bigInteger.toByteArray());
    }

    /**
     * Decodes a BigInteger from the wire buffer.
     */
    public BigInteger decodeFromWire(MutableInt pos, Buffer buf)
    {
        byte[] ar = CommonCodecs.BYTE_ARRAY.decodeFromWire(pos.intValue(), buf);
        pos.add(4 + ar.length);
        return new BigInteger(ar);
    }

    /**
     * Decodes a BigInteger from the wire buffer at the specified position.
     */
    public BigInteger decodeFromWire(int pos, Buffer buf)
    {
        return decodeFromWire(new MutableInt(pos), buf);
    }

    /**
     * Returns the BigInteger unchanged for local delivery.
     */
    public BigInteger transform(BigInteger bigInteger)
    {
        return bigInteger;
    }

    /**
     * Returns the codec name.
     */
    public String name()
    {
        return "big-integer";
    }

    /**
     * Returns the system codec ID.
     */
    public byte systemCodecID()
    {
        return -1;
    }
}
