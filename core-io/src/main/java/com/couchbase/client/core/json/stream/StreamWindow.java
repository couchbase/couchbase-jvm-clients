/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.json.stream;

/**
 * A sliding window over the contents of a byte stream.
 */
interface StreamWindow {
  /**
   * Appends the given bytes to the end of the stream.
   */
  void add(byte[] bytes, int offset, int len);

  default void add(byte[] bytes) {
    add(bytes, 0, bytes.length);
  }

  /**
   * Forgets any bytes with stream offsets lower than the given offset.
   *
   * @param offset offset relative to the start of the stream.
   * @throws IndexOutOfBoundsException if the given offset is positive and outside the window
   */
  void releaseBefore(long offset);

  /**
   * Returns a region of the stream as a byte array.
   *
   * @param startOffset region start offset relative to beginning of stream.
   * @param endOffset region end offset relative to beginning of stream.
   * @throws IndexOutOfBoundsException if the window does not contain all of the requested region
   */
  byte[] getBytes(long startOffset, long endOffset);
}
