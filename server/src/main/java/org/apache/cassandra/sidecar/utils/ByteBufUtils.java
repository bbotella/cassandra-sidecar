/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Utility class providing byte buffer manipulation and conversion operations for Cassandra Sidecar.
 * <p>
 * This class offers a comprehensive set of static utility methods for working with byte buffers,
 * input streams, and byte arrays in the context of Cassandra data processing and CDC operations.
 * The utilities are designed to handle common byte manipulation tasks including:
 * <ul>
 *   <li>ByteBuffer to byte array conversions with optimal performance</li>
 *   <li>Hexadecimal string representation of binary data</li>
 *   <li>Stream reading operations with full data consumption guarantees</li>
 *   <li>Cassandra-specific composite key splitting and building</li>
 *   <li>Length-prefixed byte buffer operations for data serialization</li>
 *   <li>UTF-8 string encoding and decoding with error handling</li>
 * </ul>
 * <p>
 * Key features include:
 * <ul>
 *   <li><strong>Memory efficiency:</strong> Optimized ByteBuffer handling that avoids
 *       unnecessary copying when dealing with heap vs direct buffers</li>
 *   <li><strong>Thread-safe string decoding:</strong> Thread-local UTF-8 decoder
 *       instances to avoid synchronization overhead</li>
 *   <li><strong>Cassandra compatibility:</strong> Support for Cassandra composite key
 *       formats including static marker handling</li>
 *   <li><strong>Robust I/O:</strong> Stream reading methods that handle partial reads
 *       and provide complete data consumption guarantees</li>
 *   <li><strong>Debugging support:</strong> Hexadecimal representation methods for
 *       binary data inspection and logging</li>
 * </ul>
 * <p>
 * The class is particularly important for CDC operations where efficient byte manipulation
 * is crucial for processing large volumes of change data. It provides the low-level
 * building blocks for serialization, deserialization, and data format conversion
 * operations throughout the sidecar system.
 * <p>
 * All methods in this class are static and thread-safe unless otherwise noted.
 * The class maintains thread-local resources (such as charset decoders) to ensure
 * optimal performance in multi-threaded environments.
 *
 * @see java.nio.ByteBuffer
 * @see java.nio.charset.CharsetDecoder
 */
public class ByteBufUtils
{
    public static final ThreadLocal<CharsetDecoder> UTF8_DECODER_PROVIDER = ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);
    public static final String EMPTY_STR = "";
    private static final int STATIC_MARKER = 0xFFFF;
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static void skipBytesFully(final DataInput in, final int bytes) throws IOException
    {
        int n = 0;
        while (n < bytes)
        {
            final int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
            {
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            }
            n += skipped;
        }
    }

    public static byte[] readRemainingBytes(final InputStream in, final int size) throws IOException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        final byte[] ar = new byte[size];
        int len;
        while ((len = in.read(ar)) != -1)
        {
            out.write(ar, 0, len);
        }
        return out.toByteArray();
    }

    public static byte[] getArray(final ByteBuffer buffer)
    {
        final int length = buffer.remaining();

        if (buffer.hasArray())
        {
            final int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        final byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    private static String toHexString(final byte[] bytes, final int length)
    {
        return toHexString(bytes, 0, length);
    }

    static String toHexString(final byte[] bytes, final int offset, final int length)
    {
        final char[] hexCharacters = new char[length << 1];

        int decimalValue;
        for (int i = offset; i < offset + length; i++)
        {
            // Calculate the int value represented by the byte
            decimalValue = bytes[i] & 0xFF;
            // Retrieve hex character for 4 upper bits
            hexCharacters[(i - offset) << 1] = HEX_ARRAY[decimalValue >> 4];
            // Retrieve hex character for 4 lower bits
            hexCharacters[((i - offset) << 1) + 1] = HEX_ARRAY[decimalValue & 0xF];
        }

        return new String(hexCharacters);
    }

    public static String toHexString(final ByteBuffer buffer)
    {
        if (buffer == null)
        {
            return "null";
        }

        if (buffer.isReadOnly())
        {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.slice().get(bytes);
            return ByteBufUtils.toHexString(bytes, bytes.length);
        }

        return ByteBufUtils.toHexString(buffer.array(),
                                        buffer.arrayOffset() + buffer.position(),
                                        buffer.remaining());
    }

    public static int readFully(final InputStream in, final byte[] b, final int len) throws IOException
    {
        if (len < 0)
        {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;
        while (n < len)
        {
            final int count = in.read(b, n, len - n);
            if (count < 0)
            {
                break;
            }
            n += count;
        }

        return n;
    }

    // changes bb position
    public static ByteBuffer readBytesWithShortLength(final ByteBuffer bb)
    {
        return readBytes(bb, readShortLength(bb));
    }

    // changes bb position
    static void writeShortLength(final ByteBuffer bb, final int length)
    {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    // Doesn't change bb position
    static int peekShortLength(final ByteBuffer bb, final int position)
    {
        final int length = (bb.get(position) & 0xFF) << 8;
        return length | (bb.get(position + 1) & 0xFF);
    }

    // changes bb position
    static int readShortLength(final ByteBuffer bb)
    {
        final int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    // changes bb position
    @SuppressWarnings("RedundantCast")
    public static ByteBuffer readBytes(final ByteBuffer bb, final int length)
    {
        final ByteBuffer copy = bb.duplicate();
        ((Buffer) copy).limit(copy.position() + length);
        ((Buffer) bb).position(bb.position() + length);
        return copy;
    }

    public static void skipFully(InputStream is, long len) throws IOException
    {
        final long skipped = is.skip(len);
        if (skipped != len)
        {
            throw new EOFException("EOF after " + skipped + " bytes out of " + len);
        }
    }

    public static ByteBuffer[] split(final ByteBuffer name, final int numKeys)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but
        // most names will be complete.
        final ByteBuffer[] l = new ByteBuffer[numKeys];
        final ByteBuffer bb = name.duplicate();
        ByteBufUtils.readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            l[i++] = readBytesWithShortLength(bb);
            bb.get(); // skip end-of-component
        }
        return i == l.length ? l : Arrays.copyOfRange(l, 0, i);
    }

    public static void readStatic(final ByteBuffer bb)
    {
        if (bb.remaining() < 2)
        {
            return;
        }

        final int header = peekShortLength(bb, bb.position());
        if ((header & 0xFFFF) != ByteBufUtils.STATIC_MARKER)
        {
            return;
        }

        readShortLength(bb); // Skip header
    }

    public static ByteBuffer build(final boolean isStatic, final ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (final ByteBuffer bb : buffers)
        {
            // 2 bytes short length + data length + 1 byte for end-of-component marker
            totalLength += 2 + bb.remaining() + 1;
        }

        final ByteBuffer out = ByteBuffer.allocate(totalLength);
        if (isStatic)
        {
            out.putShort((short) STATIC_MARKER);
        }

        for (final ByteBuffer bb : buffers)
        {
            writeShortLength(out, bb.remaining()); // short len
            out.put(bb.duplicate()); // data
            out.put((byte) 0); // end-of-component marker
        }
        out.flip();
        return out;
    }

    /**
     * Decode ByteBuffer into String using provided CharsetDecoder.
     *
     * @param buffer          byte buffer
     * @param decoderSupplier let the user provide their own CharsetDecoder provider 
     *                        e.g. using io.netty.util.concurrent.FastThreadLocal over java.lang.ThreadLocal
     * @return decoded string
     * @throws CharacterCodingException charset decoding exception
     */
    public static String string(final ByteBuffer buffer, Supplier<CharsetDecoder> decoderSupplier) throws CharacterCodingException
    {
        if (buffer.remaining() <= 0)
        {
            return EMPTY_STR;
        }
        return decoderSupplier.get().decode(buffer.duplicate()).toString();
    }

    public static String string(final ByteBuffer buffer) throws CharacterCodingException
    {
        return string(buffer, UTF8_DECODER_PROVIDER::get);
    }
}
