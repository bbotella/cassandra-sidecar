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

package org.apache.cassandra.sidecar.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link ByteBufUtils}
 */
class ByteBufUtilsTest
{
    @Test
    void testSkipBytesFully() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
        
        ByteBufUtils.skipBytesFully(dataInput, 5);
        
        // After skipping 5 bytes, next byte should be 6
        int nextByte = dataInput.readByte();
        assertThat(nextByte).isEqualTo(6);
    }
    
    @Test
    void testSkipBytesFullyEOF() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3};
        DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
        
        assertThatThrownBy(() -> ByteBufUtils.skipBytesFully(dataInput, 5))
        .isInstanceOf(EOFException.class)
        .hasMessageContaining("EOF after 3 bytes out of 5");
    }
    
    @Test
    void testReadRemainingBytes() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        byte[] result = ByteBufUtils.readRemainingBytes(inputStream, 10);
        
        assertThat(result).isEqualTo(data);
    }
    
    @Test
    void testReadRemainingBytesEmpty() throws IOException
    {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        
        byte[] result = ByteBufUtils.readRemainingBytes(inputStream, 5);
        
        assertThat(result).isEmpty();
    }
    
    @Test
    void testGetArrayFromHeapBuffer()
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.position(1);
        buffer.limit(4);
        
        byte[] result = ByteBufUtils.getArray(buffer);
        
        assertThat(result).isEqualTo(new byte[]{2, 3, 4});
    }
    
    @Test
    void testGetArrayFromDirectBuffer()
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        ByteBuffer buffer = ByteBuffer.allocateDirect(5);
        buffer.put(data);
        buffer.flip();
        buffer.position(1);
        buffer.limit(4);
        
        byte[] result = ByteBufUtils.getArray(buffer);
        
        assertThat(result).isEqualTo(new byte[]{2, 3, 4});
    }
    
    @Test
    void testToHexStringWithByteBuffer()
    {
        byte[] data = new byte[]{(byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        String result = ByteBufUtils.toHexString(buffer);
        
        assertThat(result).isEqualTo("ABCDEF");
    }
    
    @Test
    void testToHexStringWithNullBuffer()
    {
        String result = ByteBufUtils.toHexString((ByteBuffer) null);
        
        assertThat(result).isEqualTo("null");
    }
    
    @Test
    void testToHexStringWithReadOnlyBuffer()
    {
        byte[] data = new byte[]{(byte) 0x12, (byte) 0x34};
        ByteBuffer buffer = ByteBuffer.wrap(data).asReadOnlyBuffer();
        
        String result = ByteBufUtils.toHexString(buffer);
        
        assertThat(result).isEqualTo("1234");
    }
    
    @Test
    void testReadFully() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        InputStream inputStream = new ByteArrayInputStream(data);
        byte[] buffer = new byte[5];
        
        int bytesRead = ByteBufUtils.readFully(inputStream, buffer, 5);
        
        assertThat(bytesRead).isEqualTo(5);
        assertThat(buffer).isEqualTo(new byte[]{1, 2, 3, 4, 5});
    }
    
    @Test
    void testReadFullyPartial() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3};
        InputStream inputStream = new ByteArrayInputStream(data);
        byte[] buffer = new byte[5];
        
        int bytesRead = ByteBufUtils.readFully(inputStream, buffer, 5);
        
        assertThat(bytesRead).isEqualTo(3);
        assertThat(buffer).startsWith(1, 2, 3);
    }
    
    @Test
    void testReadFullyNegativeLength() throws IOException
    {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        byte[] buffer = new byte[5];
        
        assertThatThrownBy(() -> ByteBufUtils.readFully(inputStream, buffer, -1))
        .isInstanceOf(IndexOutOfBoundsException.class);
    }
    
    @Test
    void testReadBytesWithShortLength()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.putShort((short) 3); // length
        buffer.put(new byte[]{1, 2, 3}); // data
        buffer.flip();
        
        ByteBuffer result = ByteBufUtils.readBytesWithShortLength(buffer);
        
        assertThat(result.remaining()).isEqualTo(3);
        byte[] resultArray = new byte[3];
        result.get(resultArray);
        assertThat(resultArray).isEqualTo(new byte[]{1, 2, 3});
    }
    
    @Test
    void testReadBytes()
    {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
        buffer.position(1);
        
        ByteBuffer result = ByteBufUtils.readBytes(buffer, 3);
        
        assertThat(result.remaining()).isEqualTo(3);
        byte[] resultArray = new byte[3];
        result.get(resultArray);
        assertThat(resultArray).isEqualTo(new byte[]{2, 3, 4});
        assertThat(buffer.position()).isEqualTo(4); // position should advance
    }
    
    @Test
    void testSkipFully() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        ByteBufUtils.skipFully(inputStream, 5);
        
        int nextByte = inputStream.read();
        assertThat(nextByte).isEqualTo(6);
    }
    
    @Test
    void testSkipFullyEOF() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        assertThatThrownBy(() -> ByteBufUtils.skipFully(inputStream, 5))
        .isInstanceOf(EOFException.class)
        .hasMessageContaining("EOF after 3 bytes out of 5");
    }
    
    @Test
    void testSplit()
    {
        // Create a composite name with static marker
        ByteBuffer name1 = ByteBuffer.wrap("key1".getBytes());
        ByteBuffer name2 = ByteBuffer.wrap("key2".getBytes());
        ByteBuffer composite = ByteBufUtils.build(true, name1, name2);
        
        ByteBuffer[] result = ByteBufUtils.split(composite, 2);
        
        assertThat(result).hasSize(2);
        assertThat(ByteBufUtils.getArray(result[0])).isEqualTo("key1".getBytes());
        assertThat(ByteBufUtils.getArray(result[1])).isEqualTo("key2".getBytes());
    }
    
    @Test
    void testSplitFewerComponents()
    {
        ByteBuffer name1 = ByteBuffer.wrap("key1".getBytes());
        ByteBuffer composite = ByteBufUtils.build(true, name1);
        
        ByteBuffer[] result = ByteBufUtils.split(composite, 3);
        
        assertThat(result).hasSize(1);
        assertThat(ByteBufUtils.getArray(result[0])).isEqualTo("key1".getBytes());
    }
    
    @Test
    void testBuildStatic()
    {
        ByteBuffer name1 = ByteBuffer.wrap("test".getBytes());
        ByteBuffer name2 = ByteBuffer.wrap("data".getBytes());
        
        ByteBuffer result = ByteBufUtils.build(true, name1, name2);
        
        assertThat(result.remaining()).isGreaterThan(0);
        // Should start with static marker (0xFFFF)
        ByteBuffer copy = result.duplicate();
        short marker = copy.getShort();
        assertThat(marker).isEqualTo((short) 0xFFFF);
    }
    
    @Test
    void testBuildNonStatic()
    {
        ByteBuffer name1 = ByteBuffer.wrap("test".getBytes());
        
        ByteBuffer result = ByteBufUtils.build(false, name1);
        
        assertThat(result.remaining()).isGreaterThan(0);
        // Should not start with static marker
        ByteBuffer copy = result.duplicate();
        short firstBytes = copy.getShort();
        assertThat(firstBytes).isNotEqualTo((short) 0xFFFF);
    }
    
    @Test
    void testReadStatic()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.putShort((short) 0xFFFF); // static marker
        buffer.put(new byte[]{1, 2, 3});
        buffer.flip();
        
        int initialPosition = buffer.position();
        ByteBufUtils.readStatic(buffer);
        
        // Position should advance by 2 bytes (past the static marker)
        assertThat(buffer.position()).isEqualTo(initialPosition + 2);
    }
    
    @Test
    void testReadStaticNoMarker()
    {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.putShort((short) 0x1234); // not a static marker
        buffer.put(new byte[]{1, 2, 3});
        buffer.flip();
        
        int initialPosition = buffer.position();
        ByteBufUtils.readStatic(buffer);
        
        // Position should not change
        assertThat(buffer.position()).isEqualTo(initialPosition);
    }
    
    @Test
    void testReadStaticInsufficientBytes()
    {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 0x12);
        buffer.flip();
        
        int initialPosition = buffer.position();
        ByteBufUtils.readStatic(buffer);
        
        // Position should not change
        assertThat(buffer.position()).isEqualTo(initialPosition);
    }
    
    @Test
    void testStringWithValidUTF8() throws CharacterCodingException
    {
        String testString = "Hello, 世界!";
        ByteBuffer buffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
        
        String result = ByteBufUtils.string(buffer);
        
        assertThat(result).isEqualTo(testString);
    }
    
    @Test
    void testStringWithEmptyBuffer() throws CharacterCodingException
    {
        ByteBuffer buffer = ByteBuffer.allocate(0);
        
        String result = ByteBufUtils.string(buffer);
        
        assertThat(result).isEqualTo("");
    }
    
    @Test
    void testStringWithCustomDecoder() throws CharacterCodingException
    {
        String testString = "Test String";
        ByteBuffer buffer = ByteBuffer.wrap(testString.getBytes(StandardCharsets.UTF_8));
        Supplier<CharsetDecoder> decoderSupplier = () -> StandardCharsets.UTF_8.newDecoder();
        
        String result = ByteBufUtils.string(buffer, decoderSupplier);
        
        assertThat(result).isEqualTo(testString);
    }
    
    @Test
    void testStringWithInvalidUTF8()
    {
        // Create invalid UTF-8 sequence
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{(byte) 0xFF, (byte) 0xFE});
        
        assertThatThrownBy(() -> ByteBufUtils.string(buffer))
        .isInstanceOf(CharacterCodingException.class);
    }
    
    @Test
    void testHexStringConversion()
    {
        byte[] data = new byte[]{0x00, 0x01, 0x0F, (byte) 0x10, (byte) 0xFF};
        
        String result = ByteBufUtils.toHexString(ByteBuffer.wrap(data));
        
        assertThat(result).isEqualTo("00010F10FF");
    }
    
    @Test
    void testHexStringEmpty()
    {
        ByteBuffer buffer = ByteBuffer.allocate(0);
        
        String result = ByteBufUtils.toHexString(buffer);
        
        assertThat(result).isEmpty();
    }
    
    @Test
    void testComplexByteBufferOperations()
    {
        // Test a more complex scenario combining multiple operations
        ByteBuffer component1 = ByteBuffer.wrap("part1".getBytes());
        ByteBuffer component2 = ByteBuffer.wrap("part2".getBytes());
        
        // Build composite buffer
        ByteBuffer composite = ByteBufUtils.build(true, component1, component2);
        
        // Split it back
        ByteBuffer[] parts = ByteBufUtils.split(composite, 2);
        
        assertThat(parts).hasSize(2);
        assertThat(new String(ByteBufUtils.getArray(parts[0]))).isEqualTo("part1");
        assertThat(new String(ByteBufUtils.getArray(parts[1]))).isEqualTo("part2");
    }
    
    @Test
    void testReadFullyWithExactData() throws IOException
    {
        byte[] data = new byte[]{1, 2, 3, 4, 5};
        InputStream inputStream = new ByteArrayInputStream(data);
        byte[] buffer = new byte[5];
        
        int bytesRead = ByteBufUtils.readFully(inputStream, buffer, 5);
        
        assertThat(bytesRead).isEqualTo(5);
        assertThat(buffer).isEqualTo(data);
    }
    
    @Test
    void testReadFullyWithZeroLength() throws IOException
    {
        InputStream inputStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        byte[] buffer = new byte[5];
        
        int bytesRead = ByteBufUtils.readFully(inputStream, buffer, 0);
        
        assertThat(bytesRead).isEqualTo(0);
    }
}
