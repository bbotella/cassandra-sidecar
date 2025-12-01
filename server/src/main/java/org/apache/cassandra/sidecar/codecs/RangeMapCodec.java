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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableInt;

import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;

/**
 * Message codec for encoding and decoding maps of instance IP to token ranges over the Vert.x event bus.
 */
public class RangeMapCodec implements MessageCodec<Map<String, Set<TokenRange>>, Map<String, Set<TokenRange>>>
{
    public static final RangeMapCodec INSTANCE = new RangeMapCodec();

    /**
     * Encodes a range map to the wire buffer.
     */
    public void encodeToWire(Buffer buf, Map<String, Set<TokenRange>> map)
    {
        CommonCodecs.SHORT.encodeToWire(buf, (short) map.size()); // map size

        for (Map.Entry<String, Set<TokenRange>> entry : map.entrySet())
        {
            CommonCodecs.STRING.encodeToWire(buf, entry.getKey()); // instanceIP

            final Set<TokenRange> ranges = entry.getValue();
            CommonCodecs.SHORT.encodeToWire(buf, (short) ranges.size()); // numRanges
            for (TokenRange range : ranges)
            {
                RangeCodec.INSTANCE.encodeToWire(buf, range);
            }
        }
    }

    /**
     * Decodes a range map from the wire buffer.
     */
    public Map<String, Set<TokenRange>> decodeMap(MutableInt pos, Buffer buf)
    {
        int mapSize = buf.getShort(pos.intValue());
        pos.add(2);
        final Map<String, Set<TokenRange>> result = new HashMap<>(mapSize);

        for (int i = 0; i < mapSize; i++)
        {
            String ip = decodeString(pos, buf);
            int numRanges = buf.getShort(pos.intValue());
            pos.add(2);

            Set<TokenRange> ranges = new HashSet<>(numRanges);
            for (int j = 0; j < numRanges; j++)
            {
                ranges.add(RangeCodec.INSTANCE.decodeFromWire(pos, buf));
            }
            result.put(ip, ranges);
        }

        return result;
    }

    /**
     * Decodes a range map from the wire buffer at the specified position.
     */
    public Map<String, Set<TokenRange>> decodeFromWire(int pos, Buffer buf)
    {
        return decodeMap(new MutableInt(pos), buf);
    }

    /**
     * Returns an immutable copy of the map for local delivery.
     */
    public Map<String, Set<TokenRange>> transform(Map<String, Set<TokenRange>> map)
    {
        return ImmutableMap.copyOf(map);
    }

    /**
     * Returns the codec name.
     */
    public String name()
    {
        return "range-map";
    }

    /**
     * Returns the system codec ID.
     */
    public byte systemCodecID()
    {
        return -1;
    }

    protected String decodeString(MutableInt pos, Buffer buf)
    {
        int len = buf.getInt(pos.intValue());
        pos.add(4);
        byte[] bytes = buf.getBytes(pos.intValue(), pos.intValue() + len);
        pos.add(len);
        return new String(bytes, CharsetUtil.UTF_8);
    }
}
