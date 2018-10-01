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
import io.netty.buffer.ByteBufAllocator;

import java.util.Optional;

/**
 * The {@link Protocol} class holds static helpers that deal with the encoding
 * and decoding as well as access of the memcache binary protocol.
 *
 * @since 2.0.0
 */
enum Protocol {
  ;

  /**
   * Magic byte identifying a request.
   */
  static final byte MAGIC_REQUEST = (byte) 0x80;

  /**
   * The fixed header size.
   */
  static final int HEADER_SIZE = 24;

  static final int STATUS_OFFSET = 6;

  static final byte OPCODE_HELLO = 0x1f;

  static final short STATUS_SUCCESS = 0x00;

  /**
   * Create a memcached protocol request with the given params.
   *
   * @param alloc  the allocator where to allocate buffers from.
   * @param opcode the opcode used for this request.
   * @param key    the key used for this request.
   * @param body   the body used for this request.
   * @return the full request allocated and ready to use.
   */
  static ByteBuf request(final ByteBufAllocator alloc, final byte opcode,
                         final ByteBuf key, final ByteBuf body) {
    return alloc
      .buffer(HEADER_SIZE + key.readableBytes() + body.readableBytes())
      .writeByte(MAGIC_REQUEST)
      .writeByte(opcode)
      .writeShort(key.readableBytes())
      .writeByte(0) // extras length
      .writeByte(0) // data type
      .writeShort(0) // vbucket id
      .writeInt(key.readableBytes() + body.readableBytes()) // total body length
      .writeInt(0) // opaque
      .writeLong(0) // cas
      .writeBytes(key)
      .writeBytes(body);
  }

  /**
   * Returns the status of that response.
   *
   * @param response the memcache response to extract from.
   * @return the status field.
   */
  static short status(final ByteBuf response) {
    return response.getShort(STATUS_OFFSET);
  }

  /**
   * Helper method to check if the given response has a successful status.
   *
   * @param response the memcache response to extract from.
   * @return true if success.
   */
  static boolean successful(final ByteBuf response) {
    return status(response) == STATUS_SUCCESS;
  }

  static Optional<ByteBuf> body(final ByteBuf message) {
    if (message == null) {
      return Optional.empty();
    }

    int totalBodyLength = message.getInt(8);
    int keyLength = message.getShort(2);
    int extrasLength = message.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength;
    if (bodyLength > 0) {
      return Optional.of(
        message.slice(Protocol.HEADER_SIZE + extrasLength + keyLength, bodyLength)
      );
    } else {
      return Optional.empty();
    }
  }
}