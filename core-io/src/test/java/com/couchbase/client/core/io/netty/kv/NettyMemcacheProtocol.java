/*
 * Copyright 2026 Couchbase, Inc.
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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.util.Bytes;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link MemcacheProtocol} helpers that build and read packets in Netty buffers,
 * for tests (for example, as reference encoders, and in fake servers).
 */
public final class NettyMemcacheProtocol {
  private NettyMemcacheProtocol() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Create a flexible memcached protocol request with all fields necessary.
   */
  public static ByteBuf flexibleRequest(final ByteBufAllocator alloc, final Opcode opcode, final byte datatype,
                                        final short partition, final int opaque, final long cas,
                                        final ByteBuf framingExtras, final ByteBuf extras, final ByteBuf key,
                                        final ByteBuf body) {
    if (!framingExtras.isReadable()) {
      return request(alloc, opcode, datatype, partition, opaque, cas, extras, key, body);
    }

    int keySize = key.readableBytes();
    int extrasSize = extras.readableBytes();
    int framingExtrasSize = framingExtras.readableBytes();
    int totalBodySize = framingExtrasSize + extrasSize + keySize + body.readableBytes();
    return alloc
      .buffer(HEADER_SIZE + totalBodySize)
      .writeByte(Magic.FLEXIBLE_REQUEST.magic())
      .writeByte(opcode.opcode())
      .writeByte(framingExtrasSize)
      .writeByte(keySize)
      .writeByte(extrasSize)
      .writeByte(datatype)
      .writeShort(partition)
      .writeInt(totalBodySize)
      .writeInt(opaque)
      .writeLong(cas)
      .writeBytes(framingExtras)
      .writeBytes(extras)
      .writeBytes(key)
      .writeBytes(body);
  }

  /**
   * Create a regular, non-flexible memcached protocol request with all fields necessary.
   */
  public static ByteBuf request(final ByteBufAllocator alloc, final Opcode opcode, final byte datatype,
                                final short partition, final int opaque, final long cas, final ByteBuf extras,
                                final ByteBuf key, final ByteBuf body) {
    int keySize = key.readableBytes();
    int extrasSize = extras.readableBytes();
    int totalBodySize = extrasSize + keySize + body.readableBytes();
    return alloc
      .buffer(HEADER_SIZE + totalBodySize)
      .writeByte(Magic.REQUEST.magic())
      .writeByte(opcode.opcode())
      .writeShort(keySize)
      .writeByte(extrasSize)
      .writeByte(datatype)
      .writeShort(partition)
      .writeInt(totalBodySize)
      .writeInt(opaque)
      .writeLong(cas)
      .writeBytes(extras)
      .writeBytes(key)
      .writeBytes(body);
  }

  /**
   * Create a regular, non-flexible memcached protocol response with all fields necessary.
   *
   * <p>This method is mostly used for testing purposes.</p>
   */
  public static ByteBuf response(final ByteBufAllocator alloc, final Opcode opcode, final byte datatype,
                                final short status, final int opaque, final long cas, final ByteBuf extras,
                                final ByteBuf key, final ByteBuf body) {
    int keySize = key.readableBytes();
    int extrasSize = extras.readableBytes();
    int totalBodySize = extrasSize + keySize + body.readableBytes();
    return alloc
      .buffer(HEADER_SIZE + totalBodySize)
      .writeByte(Magic.RESPONSE.magic())
      .writeByte(opcode.opcode())
      .writeShort(keySize)
      .writeByte(extrasSize)
      .writeByte(datatype)
      .writeShort(status)
      .writeInt(totalBodySize)
      .writeInt(opaque)
      .writeLong(cas)
      .writeBytes(extras)
      .writeBytes(key)
      .writeBytes(body);
  }

  public static short keyLength(final ByteBuf message) {
    return isFlexible(message) ? message.getByte(3) : message.getShort(2);
  }

  public static boolean isFlexible(final ByteBuf message) {
    byte magic = magic(message);
    return magic == Magic.FLEXIBLE_REQUEST.magic() || magic == Magic.FLEXIBLE_RESPONSE.magic();
  }

  public static byte flexExtrasLength(final ByteBuf message) {
    return isFlexible(message) ? message.getByte(2) : 0;
  }

  public static byte extrasLength(final ByteBuf message) {
    return message.getByte(4);
  }

  public static byte magic(final ByteBuf message) {
    return message.getByte(0);
  }

  /**
   * Helper method to return the opcode for the given request or response.
   *
   * @param message the message to get the opcode from.
   * @return the opcode as a byte.
   */
  public static byte opcode(final ByteBuf message) {
    return message.getByte(OPCODE_OFFSET);
  }

  /**
   * Helper method to return the datatype from a request or response.
   *
   * @param message the message to get the datatype from.
   * @return the datatype as a byte.
   */
  public static byte datatype(final ByteBuf message) {
    return message.getByte(DATATYPE_OFFSET);
  }

  /**
   * Helper method to return the opaque value for the given request or response.
   *
   * @param message the message to get the opaque from.
   * @return the opaque as an int.
   */
  public static int opaque(final ByteBuf message) {
    return message.getInt(OPAQUE_OFFSET);
  }

  public static Optional<ByteBuf> rawBody(final ByteBuf message) {
    boolean flexible = message.getByte(0) == Magic.FLEXIBLE_RESPONSE.magic();

    int totalBodyLength = message.getInt(TOTAL_LENGTH_OFFSET);
    int keyLength = flexible ? message.getByte(3) : message.getShort(2);
    int flexibleExtrasLength = flexible ? message.getByte(2) : 0;
    byte extrasLength = message.getByte(4);
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

  public static String keyAsString(final ByteBuf message) {
    return key(message)
      .map(it -> it.toString(UTF_8))
      .orElse("");
  }

  public static Optional<ByteBuf> key(final ByteBuf message) {
    int keyLength = keyLength(message);
    if (keyLength > 0) {
      return Optional.of(message.slice(
        MemcacheProtocol.HEADER_SIZE + flexExtrasLength(message) + extrasLength(message),
        keyLength
      ));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the decompressed body of this message as a byte array.
   */
  public static byte[] bodyAsBytes(final ByteBuf message) {
    boolean flexible = message.getByte(0) == Magic.FLEXIBLE_RESPONSE.magic();

    int totalBodyLength = message.getInt(TOTAL_LENGTH_OFFSET);
    int keyLength = flexible ? message.getByte(3) : message.getShort(2);
    int flexibleExtrasLength = flexible ? message.getByte(2) : 0;
    byte extrasLength = message.getByte(4);
    int bodyLength = totalBodyLength - keyLength - extrasLength - flexibleExtrasLength;

    if (bodyLength > 0) {
      byte[] bytes = ByteBufUtil.getBytes(
        message,
        MemcacheProtocol.HEADER_SIZE + flexibleExtrasLength + extrasLength + keyLength,
        bodyLength
      );

      return tryDecompression(bytes, datatype(message));
    }

    return Bytes.EMPTY_BYTE_ARRAY;
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
  public static boolean verifyRequest(final ByteBuf request) {
    int readableBytes = request.readableBytes();
    if (readableBytes < MemcacheProtocol.HEADER_SIZE) {
      return false;
    }
    byte magic = request.getByte(MAGIC_OFFSET);
    int bodyPlusHeader = request.getInt(TOTAL_LENGTH_OFFSET) + MemcacheProtocol.HEADER_SIZE;
    return (magic == Magic.REQUEST.magic() || magic == Magic.FLEXIBLE_REQUEST.magic())
      && readableBytes == bodyPlusHeader;
  }

  /**
   * Returns a view of the packet in the buffer, from index 0 to the writer index (whatever the reader index),
   * without copying it, to decode a response with
   * {@link com.couchbase.client.core.msg.kv.KeyValueRequest#decode}. Uses the buffer's internal NIO buffer when it
   * has one, so it usually allocates nothing but the packet. That's safe because the packet is used only while the
   * response is decoded, and nothing else uses the buffer's internal NIO buffer meanwhile.
   */
  public static KeyValuePacket packet(ByteBuf packet) {
    int length = packet.writerIndex();
    ByteBuffer nio = packet.nioBufferCount() == 1
      ? packet.internalNioBuffer(0, length)
      : packet.nioBuffer(0, length); // several components: a copy
    return KeyValuePacket.wrap(nio);
  }

  /**
   * Helper to express no key is used for this message.
   */
  public static ByteBuf noKey() {
    return Unpooled.EMPTY_BUFFER;
  }

  /**
   * Helper to express no extras are used for this message.
   */
  public static ByteBuf noExtras() {
    return Unpooled.EMPTY_BUFFER;
  }

  /**
   * Helper to express no body is used for this message.
   */
  public static ByteBuf noBody() {
    return Unpooled.EMPTY_BUFFER;
  }

  public static ByteBuf mutationFlexibleExtras(KeyValueRequest<?> request,
                                               KeyValueChannelContext ctx,
                                               ByteBufAllocator alloc,
                                               Optional<DurabilityLevel> durabilityLevel) {
    return mutationFlexibleExtras(request, ctx, alloc, durabilityLevel, false);
  }

  public static ByteBuf mutationFlexibleExtras(KeyValueRequest<?> request,
                                               KeyValueChannelContext ctx,
                                               ByteBufAllocator alloc,
                                               Optional<DurabilityLevel> durabilityLevel,
                                               boolean preserveExpiry) {
    // Some callers represent "no synchronous durability" as Optional.empty(),
    // while others use Optional.of(DurabilityLevel.NONE).
    // Tech debt: Clean up that discrepancy.
    // Short term: Canonicalize here.
    DurabilityLevel durability = durabilityLevel.orElse(DurabilityLevel.NONE);

    if (durability == DurabilityLevel.NONE && !preserveExpiry) {
      return Unpooled.EMPTY_BUFFER;
    }

    ByteBuf result = alloc.buffer(5);
    try {
      if (preserveExpiry) {
        if (!ctx.preserveTtl()) {
          throw new FeatureNotAvailableException(
              "This version of Couchbase Server does not support preserving expiry when modifying documents.");
        }

        result.writeByte(PRESERVE_TTL_FLEXIBLE_IDENT);
      }

      if (durability != DurabilityLevel.NONE) {
        if (!ctx.syncReplicationEnabled()) {
          throw new DurabilityLevelNotAvailableException(KeyValueErrorContext.incompleteRequest(request));
        }
        flexibleSyncReplication(result, durability, request.timeout(), request.context());
      }

      return result;

    } catch (Throwable t) {
      ReferenceCountUtil.release(result);
      throw t;
    }
  }

  /**
   * Helper method to write the flexible extras for sync replication.
   * <p>
   * Note that this method writes a short value from an integer deadline. The netty method will make sure to
   * only look at the lower 16 bits - this allows us to write an unsigned short!
   *
   * @param buffer the buffer to write to.
   * @param type the type of sync replication.
   * @param timeout the timeout to use.
   * @param ctx the core context to use.
   * @return the same buffer
   */
  static ByteBuf flexibleSyncReplication(ByteBuf buffer, DurabilityLevel type,
                                         Duration timeout, CoreContext ctx) {
    return buffer
      .writeByte(SYNC_REPLICATION_FLEXIBLE_IDENT | (byte) 0x03)
      .writeByte(type.code())
      .writeShort(syncReplicationDeadline(timeout, ctx));
  }

  /**
   * Parses the server duration from the frame.
   *
   * It reads through the byte stream looking for the tracing frame and if
   * found extracts the info. If a different frame is found it is just
   * skipped.
   *
   * Per algorithm, the found server duration is round up/down using
   * the {@link Math#round(float)} function and has microsecond
   * precision.
   *
   * @param response the response to extract it from.
   * @return the extracted duration, 0 if not found.
   */
  public static long parseServerDurationFromResponse(final ByteBuf response) {
    int flexibleExtrasLength = response.getByte(0) == Magic.FLEXIBLE_RESPONSE.magic()
      ? response.getByte(2)
      : 0;

    if (flexibleExtrasLength > 0) {
      for (int offset = 0; offset < flexibleExtrasLength; offset++) {
        byte control = response.getByte(MemcacheProtocol.HEADER_SIZE + offset);
        byte id = (byte) (control & 0xF0);
        byte len = (byte) (control & 0x0F);
        if (id == FRAMING_EXTRAS_TRACING) {
          return Math.round(
            Math.pow(response.getUnsignedShort(MemcacheProtocol.HEADER_SIZE + offset + 1), 1.74) / 2
          );
        }
        offset += len;
      }
    }

    return 0;
  }
}
