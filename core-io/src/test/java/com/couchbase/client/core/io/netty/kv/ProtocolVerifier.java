/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.kv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

/**
 * Allows to verify certain memcached protocol packets.
 *
 * @since 2.0.0
 */
public class ProtocolVerifier {

  /**
   * Helper method which very simply strips apart the pretty hex dump
   * format and turns it into something useful for testing.
   *
   * @param dump the pretty input dump.
   * @return the decoded byte array.
   */
  public static ByteBuf decodeHexDump(final String dump) {
    StringBuilder rawDump = new StringBuilder();
    for (String line : dump.split("\\r?\\n")) {
      if (line.startsWith("|")) {
        String[] parts = line.split("\\|");
        String sequence = parts[2];
        String stripped = sequence.replaceAll("\\s", "");
        rawDump.append(stripped);
      }
    }
    return Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(rawDump));
  }
}
