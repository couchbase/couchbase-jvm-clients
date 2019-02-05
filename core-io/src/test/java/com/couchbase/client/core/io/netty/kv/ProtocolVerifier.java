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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.util.Optional;

/**
 * Allows to verify certain memcached protocol packets.
 *
 * @since 2.0.0
 */
public class ProtocolVerifier {

  /**
   * Sanity checks based on some criteria given.
   *
   * @param request          the request to verify.
   * @param opcode           the opcode that should be used.
   * @param shouldHaveKey    if this request is supposed to have a key.
   * @param shouldHaveExtras if this request is supposed to have extras.
   * @param shouldHaveBody   if this request is supposed to have body.
   */
  static void verifyRequest(final ByteBuf request, final byte opcode, final boolean shouldHaveKey,
                            final boolean shouldHaveExtras, final boolean shouldHaveBody) {
    // 1: Verify header integrity
    assertTrue(
      request.readableBytes() >= MemcacheProtocol.HEADER_SIZE,
      "header size < " + MemcacheProtocol.HEADER_SIZE
    );

    int totalBodyLength = request.getInt(8);
    int totalLength = MemcacheProtocol.HEADER_SIZE + totalBodyLength;
    assertTrue(
      request.readableBytes() >= totalLength,
      "header + body size < " + totalLength
    );

    assertEquals(
      MemcacheProtocol.Magic.REQUEST.magic(),
      request.getByte(0),
      "request magic does not match"
    );
    assertEquals(opcode, request.getByte(1), "opcode does not match");

    int keyLength = request.getShort(2);
    int extrasLength = request.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength;

    // 2: If set, verify key integrity
    if (shouldHaveKey) {
      assertTrue(keyLength > 0, "should have key but key length is 0");
      assertEquals(keyLength, key(request).get().readableBytes());
    } else {
      assertEquals(0, keyLength, "should not have key but key length is > 0");
    }

    // 3: If set, verify extras integrity
    if (shouldHaveExtras) {
      assertTrue(extrasLength > 0, "should have extras but extras length is 0");
      assertEquals(extrasLength, extras(request).get().readableBytes());
    } else {
      assertEquals(0, extrasLength, "should not have extras but extras length is > 0");
    }

    // 4: If set, verify body integrity
    if (shouldHaveBody) {
      assertTrue(bodyLength > 0, "should have body but body length is 0");
      assertEquals(bodyLength, body(request).get().readableBytes());
    } else {
      assertEquals(0, bodyLength, "should not have body but body length is > 0");
    }
  }

  static Optional<ByteBuf> key(final ByteBuf request) {
    int keyLength = request.getShort(2);
    if (keyLength > 0) {
      int extrasLength = request.getByte(4);
      return Optional.of(request.slice(MemcacheProtocol.HEADER_SIZE + extrasLength, keyLength));
    } else {
      return Optional.empty();
    }
  }

  static Optional<ByteBuf> extras(final ByteBuf request) {
    int extrasLength = request.getByte(4);
    if (extrasLength > 0) {
      return Optional.of(request.slice(MemcacheProtocol.HEADER_SIZE, extrasLength));
    } else {
      return Optional.empty();
    }
  }

  static Optional<ByteBuf> body(final ByteBuf request) {
    int totalBodyLength = request.getInt(8);
    int keyLength = request.getShort(2);
    int extrasLength = request.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength;
    if (bodyLength > 0) {
      return Optional.of(
        request.slice(MemcacheProtocol.HEADER_SIZE + extrasLength + keyLength, bodyLength)
      );
    } else {
      return Optional.empty();
    }
  }

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