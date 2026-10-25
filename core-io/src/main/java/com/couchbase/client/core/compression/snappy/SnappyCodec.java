/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.compression.snappy;

import com.couchbase.client.core.annotation.Stability;

@Stability.Internal
public interface SnappyCodec {
  /**
   * Returns the fastest implementation compatible with the runtime environment.
   */
  static SnappyCodec instance() {
    return SnappyHelper.INSTANCE;
  }

  byte[] compress(byte[] uncompressed);

  /**
   * Returns a compressor that reuses its working memory from one call to the next,
   * instead of allocating it each time like {@link #compress(byte[])} might.
   * <p>
   * The compressor is not thread-safe.
   */
  Compressor newCompressor();

  /**
   * Returns the most {@link Compressor#compress} can write when compressing {@code uncompressedLength} bytes.
   */
  int maxCompressedLength(int uncompressedLength);

  default byte[] decompress(byte[] compressed) {
    return decompress(compressed, 0, compressed.length);
  }

  byte[] decompress(byte[] compressed, int offset, int len);

  /**
   * Not thread-safe.
   *
   * @see #newCompressor()
   */
  interface Compressor {
    /**
     * Compresses {@code inputLength} bytes of {@code input} (from {@code inputOffset}) into {@code output}
     * (from {@code outputOffset}), which must have room for {@link SnappyCodec#maxCompressedLength}.
     *
     * @return the compressed length
     */
    int compress(
      byte[] input,
      int inputOffset,
      int inputLength,
      byte[] output,
      int outputOffset
    );
  }
}
