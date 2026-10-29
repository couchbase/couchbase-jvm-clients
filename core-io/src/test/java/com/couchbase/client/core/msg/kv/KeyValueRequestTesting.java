/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestWriter;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.msg.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Helpers for testing KV requests: encoding with both encoders, and decoding every way.
 */
final class KeyValueRequestTesting {
  static final Duration TIMEOUT = Duration.ofMillis(2500);
  static final CoreContext CORE_CONTEXT = mock(CoreContext.class);
  static final CollectionIdentifier DEFAULT_COLLECTION = CollectionIdentifier.fromDefault("b");
  static final CollectionIdentifier CUSTOM_COLLECTION = new CollectionIdentifier("b", Optional.of("s"), Optional.of("c"));
  static final UnpooledByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
  static final int OPAQUE = 0x12345678;
  static final short PARTITION = 42;

  private KeyValueRequestTesting() {
    throw new AssertionError("not instantiable");
  }

  static void assertEncoding(KeyValueRequest<?> request, KeyValueChannelContext ctx, byte[] expected) throws IOException {
    request.partition(PARTITION);
    assertEncodingAsConstructed(request, ctx, expected);
  }

  /**
   * Like {@link #assertEncoding}, but leaves the request's partition alone (for a request that's constructed with
   * its partition, and can't be given another).
   */
  static void assertEncodingAsConstructed(
    KeyValueRequest<?> request,
    KeyValueChannelContext ctx,
    byte[] expected
  ) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    KeyValueRequestWriter writer = newWriter(out, ctx);
    writer.makeRoomForRequest();
    request.encodeTo(writer);
    writer.completeRequestHead(OPAQUE);
    assertArrayEquals(expected, writer.requestBytes());
    writer.finishRequest();
    writer.flush();
    assertArrayEquals(expected, out.toByteArray());
    assertTrue(NettyMemcacheProtocol.verifyRequest(Unpooled.wrappedBuffer(expected)));
  }

  /**
   * Encodes the request with a new writer. Throws if the request can't be encoded.
   */
  static ByteBuf encode(KeyValueRequest<?> request, KeyValueChannelContext ctx) {
    KeyValueRequestWriter writer = newWriter(new ByteArrayOutputStream(), ctx);
    try {
      writer.makeRoomForRequest();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    request.encodeTo(writer);
    writer.completeRequestHead(OPAQUE);
    return Unpooled.wrappedBuffer(writer.requestBytes());
  }

  /**
   * Decodes the response directly from a packet, and from packet views of a heap
   * buffer, of direct buffers, and of a composite buffer.
   */
  static <R extends Response> List<R> decodeEveryWay(
    KeyValueRequest<R> request,
    byte[] packet,
    KeyValueChannelContext ctx
  ) {
    byte[] padded = new byte[packet.length + 10];
    System.arraycopy(packet, 0, padded, 3, packet.length);
    R fromPacket = request.decode(KeyValuePacket.wrap(padded, 3, packet.length), ctx);

    R fromHeap = request.decode(NettyMemcacheProtocol.packet(Unpooled.wrappedBuffer(packet)), ctx);

    ByteBuf direct = Unpooled.directBuffer(packet.length).writeBytes(packet);
    ByteBuf pooled = PooledByteBufAllocator.DEFAULT.directBuffer(packet.length).writeBytes(packet);
    pooled.readerIndex(5); // ignored, like MemcacheProtocol's readers
    try {
      R fromDirect = request.decode(NettyMemcacheProtocol.packet(direct), ctx);
      R fromPooled = request.decode(NettyMemcacheProtocol.packet(pooled), ctx);
      // Several components (no single internal NIO buffer).
      int half = packet.length / 2;
      R fromComposite = request.decode(NettyMemcacheProtocol.packet(Unpooled.wrappedBuffer(
        Unpooled.wrappedBuffer(packet, 0, half), Unpooled.wrappedBuffer(packet, half, packet.length - half))), ctx);
      return Arrays.asList(fromPacket, fromHeap, fromDirect, fromPooled, fromComposite);
    } finally {
      direct.release();
      pooled.release();
    }
  }

  static <R extends Response> List<R> decodeEveryWay(
    KeyValueRequest<R> request,
    byte[] packet
  ) {
    return decodeEveryWay(request, packet, ctx(null, EnumSet.noneOf(ServerFeature.class)));
  }

  static byte[] response(Opcode opcode, byte datatype, short status, long cas,
                                 byte[] extras, byte[] key, byte[] body) {
    ByteBuf packet = NettyMemcacheProtocol.response(ALLOC, opcode, datatype, status, OPAQUE, cas,
      Unpooled.wrappedBuffer(extras), Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(body));
    try {
      return ByteBufUtil.getBytes(packet);
    } finally {
      packet.release();
    }
  }

  static byte[] ints(int value) {
    byte[] bytes = new byte[4];
    Unpooled.wrappedBuffer(bytes).setInt(0, value);
    return bytes;
  }

  static KeyValueRequestWriter newWriter(
    ByteArrayOutputStream out,
    KeyValueChannelContext ctx
  ) {
    return new KeyValueRequestWriter(out, 64 * 1024, ctx);
  }

  static KeyValueChannelContext compressingCtx() {
    return ctx(CompressionConfig.builder().enable(true).build(), EnumSet.of(ServerFeature.COLLECTIONS));
  }

  static KeyValueChannelContext ctx(CompressionConfig compression, Set<ServerFeature> features) {
    CollectionMap collectionMap = new CollectionMap();
    collectionMap.put(CUSTOM_COLLECTION, new byte[]{0x09});
    return new KeyValueChannelContext(compression, Optional.of("b"), collectionMap, null, features);
  }

  static byte[] bytes(ByteBuf buf) {
    try {
      return ByteBufUtil.getBytes(buf);
    } finally {
      buf.release();
    }
  }
}
