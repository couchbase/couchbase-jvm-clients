/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * THIS FILE HAS BEEN MODIFIED FROM THE ORIGINAL VERSION.
 *
 * The original came from https://github.com/dain/snappy/ at tag "snappy-0.4".
 *
 * It was relocated into a different package to avoid conflicts with
 * the original.
 *
 * It was renamed from `Snappy` to `SlowSnappy`.
 *
 * It was annotated as Couchbase internal API.
 */
package com.couchbase.client.core.compression.snappy.repackaged.org.iq80.snappy.v04;

import com.couchbase.client.core.annotation.Stability;

import java.util.Arrays;

@Stability.Internal
public final class SlowSnappy
{

    private SlowSnappy()
    {
    }

    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
            throws CorruptionException
    {
        return SnappyDecompressor.getUncompressedLength(compressed, compressedOffset);
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
            throws CorruptionException
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize);
    }

    public static int uncompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize, uncompressed, uncompressedOffset);
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return SnappyCompressor.maxCompressedLength(sourceLength);
    }

    public static int compress(
            byte[] uncompressed,
            int uncompressedOffset,
            int uncompressedLength,
            byte[] compressed,
            int compressedOffset)
    {
        return SnappyCompressor.compress(uncompressed,
                uncompressedOffset,
                uncompressedLength,
                compressed,
                compressedOffset);
    }


    public static byte[] compress(byte[] data)
    {
        byte[] compressedOut = new byte[maxCompressedLength(data.length)];
        int compressedSize = compress(data, 0, data.length, compressedOut, 0);
        byte[] trimmedBuffer = Arrays.copyOf(compressedOut, compressedSize);
        return trimmedBuffer;
    }

    static final int LITERAL = 0;
    static final int COPY_1_BYTE_OFFSET = 1;  // 3 bit length + 3 bits of offset in opcode
    static final int COPY_2_BYTE_OFFSET = 2;
    static final int COPY_4_BYTE_OFFSET = 3;
}
