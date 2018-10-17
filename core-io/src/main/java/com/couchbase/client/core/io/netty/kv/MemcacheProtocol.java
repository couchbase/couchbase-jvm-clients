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
 * The {@link MemcacheProtocol} class holds static helpers that deal with the encoding
 * and decoding as well as access of the memcache binary protocol.
 *
 * @since 2.0.0
 */
enum MemcacheProtocol {
  ;

  /**
   * Magic byte identifying a request.
   */
  static final byte MAGIC_REQUEST = (byte) 0x80;

  /**
   * Magic byte for a response with flexible framing extras.
   */
  static final byte MAGIC_FLEXIBLE = (byte) 0x18;

  /**
   * Magic byte for a response without flexible framing extras.
   */
  static final byte MAGIC_RESPONSE = (byte) 0x81;

  /**
   * The fixed header size.
   */
  static final int HEADER_SIZE = 24;

  /**
   * The offset of the magic byte.
   */
  static final int MAGIC_OFFSET = 0;

  /**
   * The offset for the opcode.
   */
  static final int OPCODE_OFFSET = 1;

  /**
   * The offset of the status field.
   */
  static final int STATUS_OFFSET = 6;

  /**
   * The offset of the total length field.
   */
  static final int TOTAL_LENGTH_OFFSET = 8;

  static final short STATUS_SUCCESS = 0x00;

  /**
   * Create a memcached protocol request with key and body.
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
   * Create a memcached protocol request with body only.
   *
   * @param alloc  the allocator where to allocate buffers from.
   * @param opcode the opcode used for this request.
   * @param body   the body used for this request.
   * @return the full request allocated and ready to use.
   */
  static ByteBuf request(final ByteBufAllocator alloc, final byte opcode,
                         final ByteBuf body) {
    return alloc
      .buffer(HEADER_SIZE + body.readableBytes())
      .writeByte(MAGIC_REQUEST)
      .writeByte(opcode)
      .writeShort(0)
      .writeByte(0) // extras length
      .writeByte(0) // data type
      .writeShort(0) // vbucket id
      .writeInt(body.readableBytes()) // total body length
      .writeInt(0) // opaque
      .writeLong(0) // cas
      .writeBytes(body);
  }

  /**
   * Returns the status of that response.
   *
   * @param message the memcache message to extract from.
   * @return the status field.
   */
  static short status(final ByteBuf message) {
    return message.getShort(STATUS_OFFSET);
  }

  /**
   * Helper method to check if the given response has a successful status.
   *
   * @param message the memcache message to extract from.
   * @return true if success.
   */
  static boolean successful(final ByteBuf message) {
    return status(message) == STATUS_SUCCESS;
  }

  /**
   * Helper method to return the opcode for the given request or response.
   *
   * @param message the message to get the opcode from.
   * @return the opcode as a byte.
   */
  static byte opcode(final ByteBuf message) {
    return message.getByte(OPCODE_OFFSET);
  }

  /**
   * Returns the body of the message if available.
   *
   * @param message the message of the body or empty if none found.
   * @return an optional either containing the body of the message or none.
   */
  static Optional<ByteBuf> body(final ByteBuf message) {
    if (message == null) {
      return Optional.empty();
    }
    boolean flexible = message.getByte(0) == MAGIC_FLEXIBLE;

    int totalBodyLength = message.getInt(8);
    int keyLength = flexible ? message.getByte(3) : message.getShort(2);
    int flexibleExtrasLength = flexible ? message.getByte(2) : 0;
    int extrasLength = message.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength - flexibleExtrasLength;
    if (bodyLength > 0) {
      return Optional.of(message.slice(
          MemcacheProtocol.HEADER_SIZE + flexibleExtrasLength + extrasLength + keyLength,
          bodyLength
      ));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Performs simple sanity checking of a key/value request.
   *
   * It checks the magic byte and if the total readable bytes match
   * up with the total length of the packet.
   *
   * @param request the request to check.
   * @return true if verified, false otherwise.
   */
  static boolean verifyRequest(final ByteBuf request) {
    int readableBytes = request.readableBytes();
    if (readableBytes < MemcacheProtocol.HEADER_SIZE) {
      return false;
    }
    byte magic = request.getByte(MAGIC_OFFSET);
    int bodyPlusHeader = request.getInt(TOTAL_LENGTH_OFFSET) + MemcacheProtocol.HEADER_SIZE;
    return magic == MemcacheProtocol.MAGIC_REQUEST && readableBytes == bodyPlusHeader;
  }

  /**
   * Performs simple sanity checking of a key/value response.
   *
   * It checks the magic byte and if the total readable bytes match
   * up with the total length of the packet.
   *
   * @param response the response to check.
   * @return true if verified, false otherwise.
   */
  static boolean verifyResponse(final ByteBuf response) {
    int readableBytes = response.readableBytes();
    if (readableBytes < MemcacheProtocol.HEADER_SIZE) {
      return false;
    }
    byte magic = response.getByte(MAGIC_OFFSET);
    int bodyPlusHeader = response.getInt(TOTAL_LENGTH_OFFSET) + MemcacheProtocol.HEADER_SIZE;

    return
      (magic == MemcacheProtocol.MAGIC_FLEXIBLE || magic == MemcacheProtocol.MAGIC_RESPONSE)
      && readableBytes == bodyPlusHeader;
  }

  /**
   * Contains all known/used kv protocol opcodes.
   */
  enum Opcode {
    /**
     * The hello command used during bootstrap to negoatiate the features.
     */
    HELLO((byte) 0x1f),
    /**
     * Command used to fetch the error map during the bootstrap process.
     */
    ERROR_MAP((byte) 0xfe),
    /**
     * Command used to select a specific bucket on a connection.
     */
    SELECT_BUCKET((byte) 0x89),
    /**
     * List all SASL auth mechanisms the server supports.
     */
    SASL_LIST_MECHS((byte) 0x20),
    /**
     * Initial auth step in the SASL negotiation.
     */
    SASL_AUTH((byte) 0x21),
    /**
     * Subsequent steps in the SASL negotiation.
     */
    SASL_STEP((byte) 0x22);

    private final byte opcode;

    Opcode(byte opcode) {
      this.opcode = opcode;
    }

    /**
     * Returns the opcode for the given command.
     *
     * @return the opcode for the command.
     */
    public byte opcode() {
      return opcode;
    }

  }
}