/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.codecs;

import java.math.BigInteger;

import com.google.common.collect.BoundType;
import org.apache.commons.lang3.mutable.MutableInt;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;

/**
 * Message codec for encoding and decoding TokenRange objects over the Vert.x event bus.
 */
public class RangeCodec implements MessageCodec<TokenRange, TokenRange>
{
    public static final RangeCodec INSTANCE = new RangeCodec();

    /**
     * Encodes a TokenRange to the wire buffer.
     */
    public void encodeToWire(Buffer buf, TokenRange range)
    {
        encodeType(buf, range.range.lowerBoundType());
        BigIntegerCodec.INSTANCE.encodeToWire(buf, range.range.lowerEndpoint().toBigInteger());
        encodeType(buf, range.range.upperBoundType());
        BigIntegerCodec.INSTANCE.encodeToWire(buf, range.range.upperEndpoint().toBigInteger());
    }

    /**
     * Decodes a TokenRange from the wire buffer.
     */
    public TokenRange decodeFromWire(MutableInt pos, Buffer buf)
    {
        BigInteger lower = BigIntegerCodec.INSTANCE.decodeFromWire(pos, buf);

        BigInteger upper = BigIntegerCodec.INSTANCE.decodeFromWire(pos, buf);

        return new TokenRange(lower, upper);
    }

    /**
     * Decodes a TokenRange from the wire buffer at the specified position.
     */
    public TokenRange decodeFromWire(int pos, Buffer buf)
    {
        return decodeFromWire(new MutableInt(pos), buf);
    }

    protected static void encodeType(Buffer buf, BoundType type)
    {
        CommonCodecs.BOOL.encodeToWire(buf, type == BoundType.OPEN);
    }

    protected static BoundType decodeType(MutableInt pos, Buffer buf)
    {
        return CommonCodecs.BOOL.decodeFromWire(pos.getAndIncrement(), buf) ? BoundType.OPEN : BoundType.CLOSED;
    }

    /**
     * Returns the TokenRange unchanged for local delivery.
     */
    public TokenRange transform(TokenRange range)
    {
        return range;
    }

    /**
     * Returns the codec name.
     */
    public String name()
    {
        return "big-integer-range";
    }

    /**
     * Returns the system codec ID.
     */
    public byte systemCodecID()
    {
        return -1;
    }
}
