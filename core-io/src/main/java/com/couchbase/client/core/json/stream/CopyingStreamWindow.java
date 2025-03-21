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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A stream window implementation that copies input data into a single accumulator buffer.
 */
class CopyingStreamWindow implements StreamWindow {
  private final Buffer window = new Buffer();

  /**
   * Offset from the beginning of the stream to the end of the window.
   */
  private long streamOffset;

  @Override
  public void add(byte[] bytes, int offset, int len) {
    window.writeBytes(bytes, offset, len);
    streamOffset += len;
  }

  @Override
  public void releaseBefore(long releaseStreamOffset) {
    if (releaseStreamOffset <= 0) {
      return;
    }

    int localOffset = toLocalOffset(releaseStreamOffset);
    window.skipBytes(localOffset);
    window.discardSomeReadBytes();
  }

  @Override
  public byte[] getBytes(long startStreamOffset, long endStreamOffset) {
    final int localStartOffset = toLocalOffset(startStreamOffset);
    final int localEndOffset = toLocalOffset(endStreamOffset);
    final byte[] result = new byte[localEndOffset - localStartOffset];
    window.getBytes(window.readerIndex() + localStartOffset, result);
    return result;
  }

  /**
   * @param streamOffset offset from the beginning of the stream
   * @return corresponding offset from window's reader index
   */
  private int toLocalOffset(long streamOffset) {
    return (int) (streamOffset - this.streamOffset + window.readableBytes());
  }

  @Override
  public String toString() {
    return "CopyingStreamWindow{" +
            "window=" + window +
            ", streamOffset=" + streamOffset +
            ", windowContents='" + window.toString(UTF_8) + "'" +
            '}';
  }
}
