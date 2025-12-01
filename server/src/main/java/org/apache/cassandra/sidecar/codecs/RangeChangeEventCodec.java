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

import org.apache.commons.lang3.mutable.MutableInt;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import org.apache.cassandra.sidecar.coordination.RangeManager;

/**
 * Message codec for encoding and decoding token range change events over the Vert.x event bus.
 */
public class RangeChangeEventCodec implements MessageCodec<RangeManager.RangeChangeEvent, RangeManager.RangeChangeEvent>
{
    public static final RangeChangeEventCodec INSTANCE = new RangeChangeEventCodec();

    /**
     * Encodes a range change event to the wire buffer.
     */
    public void encodeToWire(Buffer buf, RangeManager.RangeChangeEvent event)
    {
        RangeMapCodec.INSTANCE.encodeToWire(buf, event.change);
        RangeMapCodec.INSTANCE.encodeToWire(buf, event.newView);
    }

    /**
     * Decodes a range change event from the wire buffer.
     */
    public RangeManager.RangeChangeEvent decodeFromWire(int pos, Buffer buf)
    {
        final MutableInt mutablePos = new MutableInt(pos);
        return new RangeManager.RangeChangeEvent(RangeMapCodec.INSTANCE.decodeMap(mutablePos, buf), RangeMapCodec.INSTANCE.decodeMap(mutablePos, buf));
    }

    /**
     * Returns the event unchanged for local delivery.
     */
    public RangeManager.RangeChangeEvent transform(RangeManager.RangeChangeEvent event)
    {
        return event; // immutable
    }

    /**
     * Returns the codec name.
     */
    public String name()
    {
        return "range-change-event";
    }

    /**
     * Returns the system codec ID.
     */
    public byte systemCodecID()
    {
        return -1;
    }
}
