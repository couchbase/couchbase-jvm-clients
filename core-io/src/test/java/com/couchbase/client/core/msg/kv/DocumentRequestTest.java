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

import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.DurabilityLevelNotAvailableException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mutationFlexibleExtras;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.ALLOC;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.CORE_CONTEXT;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.CUSTOM_COLLECTION;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.OPAQUE;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.PARTITION;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.TIMEOUT;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.assertEncoding;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.bytes;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.compressingCtx;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.ctx;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.decodeEveryWay;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.ints;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.newWriter;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.response;
import static com.couchbase.client.test.Util.readResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the document requests other than Get, Upsert and the sub-document ones: each encodes, with both
 * encoders, the same packet as its old {@code ByteBuf} encoder (built here with {@link MemcacheProtocol}'s helpers,
 * as the reference), and decodes its response.
 */
class DocumentRequestTest {
  private static final String KEY = "some-key";
  private static final byte[] CONTENT = "{\"hello\":\"world\"}".getBytes(UTF_8);
  private static final byte[] LONG_CONTENT = readResource("dummy.json", DocumentRequestTest.class).getBytes(UTF_8);
  private static final int FLAGS = 0x02000006;
  private static final long EXPIRY = 3600;
  private static final long CAS = 0x1234_5678_9abcL;
  private static final Optional<DurabilityLevel> NO_DURABILITY = Optional.empty();
  private static final Optional<DurabilityLevel> MAJORITY = Optional.of(DurabilityLevel.MAJORITY);

  private static final KeyValueChannelContext COLLECTIONS = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
  private static final KeyValueChannelContext DURABLE = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS,
    ServerFeature.SYNC_REPLICATION, ServerFeature.ALT_REQUEST, ServerFeature.PRESERVE_TTL));

  // Encoding

  @Test
  void insert() throws IOException {
    assertEncoding(insert(CONTENT, NO_DURABILITY), COLLECTIONS,
      reference(insert(CONTENT, NO_DURABILITY), COLLECTIONS, Opcode.ADD, 0, 0, NO_DURABILITY, false, flagsAndExpiry(), CONTENT));
    assertEncoding(insert(CONTENT, MAJORITY), DURABLE,
      reference(insert(CONTENT, MAJORITY), DURABLE, Opcode.ADD, 0, 0, MAJORITY, false, flagsAndExpiry(), CONTENT));
  }

  @Test
  void insertCompressed() throws IOException {
    InsertRequest request = insert(LONG_CONTENT, NO_DURABILITY);
    assertEncoding(request, compressingCtx(), referenceCompressed(request, compressingCtx(), Opcode.ADD, 0, NO_DURABILITY, false, flagsAndExpiry(), LONG_CONTENT));
  }

  @Test
  void replace() throws IOException {
    assertEncoding(replace(CONTENT, false, NO_DURABILITY), COLLECTIONS,
      reference(replace(CONTENT, false, NO_DURABILITY), COLLECTIONS, Opcode.REPLACE, 0, CAS, NO_DURABILITY, false, flagsAndExpiry(), CONTENT));
    assertEncoding(replace(CONTENT, true, MAJORITY), DURABLE,
      reference(replace(CONTENT, true, MAJORITY), DURABLE, Opcode.REPLACE, 0, CAS, MAJORITY, true,
        Unpooled.buffer().writeInt(FLAGS).writeInt(0), CONTENT));
  }

  @Test
  void replaceCompressed() throws IOException {
    ReplaceRequest request = replace(LONG_CONTENT, false, NO_DURABILITY);
    assertEncoding(request, compressingCtx(), referenceCompressed(request, compressingCtx(), Opcode.REPLACE, CAS, NO_DURABILITY, false, flagsAndExpiry(), LONG_CONTENT));
  }

  @Test
  void appendAndPrepend() throws IOException {
    assertEncoding(append(CONTENT, NO_DURABILITY), COLLECTIONS,
      reference(append(CONTENT, NO_DURABILITY), COLLECTIONS, Opcode.APPEND, 0, CAS, NO_DURABILITY, false, null, CONTENT));
    assertEncoding(prepend(CONTENT, MAJORITY), DURABLE,
      reference(prepend(CONTENT, MAJORITY), DURABLE, Opcode.PREPEND, 0, CAS, MAJORITY, false, null, CONTENT));
  }

  @Test
  void appendAndPrependCompressed() throws IOException {
    AppendRequest append = append(LONG_CONTENT, NO_DURABILITY);
    assertEncoding(append, compressingCtx(), referenceCompressed(append, compressingCtx(), Opcode.APPEND, CAS, NO_DURABILITY, false, null, LONG_CONTENT));
    PrependRequest prepend = prepend(LONG_CONTENT, NO_DURABILITY);
    assertEncoding(prepend, compressingCtx(), referenceCompressed(prepend, compressingCtx(), Opcode.PREPEND, CAS, NO_DURABILITY, false, null, LONG_CONTENT));
  }

  @Test
  void remove() throws IOException {
    assertEncoding(remove(NO_DURABILITY), COLLECTIONS,
      reference(remove(NO_DURABILITY), COLLECTIONS, Opcode.DELETE, 0, CAS, NO_DURABILITY, false, null, null));
    assertEncoding(remove(MAJORITY), DURABLE,
      reference(remove(MAJORITY), DURABLE, Opcode.DELETE, 0, CAS, MAJORITY, false, null, null));
  }

  @Test
  void touchAndUnlock() throws IOException {
    TouchRequest touch = new TouchRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, EXPIRY, null);
    assertEncoding(touch, COLLECTIONS,
      reference(touch, COLLECTIONS, Opcode.TOUCH, 0, 0, NO_DURABILITY, false, Unpooled.buffer().writeInt((int) EXPIRY), null));
    UnlockRequest unlock = new UnlockRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, CAS, null);
    assertEncoding(unlock, COLLECTIONS, reference(unlock, COLLECTIONS, Opcode.UNLOCK, 0, CAS, NO_DURABILITY, false, null, null));
  }

  @Test
  void incrementAndDecrement() throws IOException {
    assertEncoding(increment(Optional.of(10L), NO_DURABILITY), COLLECTIONS,
      reference(increment(Optional.of(10L), NO_DURABILITY), COLLECTIONS, Opcode.INCREMENT, 0, 0, NO_DURABILITY, false,
        counterExtras(5, 10, (int) EXPIRY), null));
    assertEncoding(increment(Optional.empty(), MAJORITY), DURABLE,
      reference(increment(Optional.empty(), MAJORITY), DURABLE, Opcode.INCREMENT, 0, 0, MAJORITY, false,
        counterExtras(5, 0, IncrementRequest.COUNTER_NOT_EXISTS_EXPIRY), null));
    assertEncoding(decrement(Optional.of(10L), NO_DURABILITY), COLLECTIONS,
      reference(decrement(Optional.of(10L), NO_DURABILITY), COLLECTIONS, Opcode.DECREMENT, 0, 0, NO_DURABILITY, false,
        counterExtras(5, 10, (int) EXPIRY), null));
    assertEncoding(decrement(Optional.empty(), NO_DURABILITY), COLLECTIONS,
      reference(decrement(Optional.empty(), NO_DURABILITY), COLLECTIONS, Opcode.DECREMENT, 0, 0, NO_DURABILITY, false,
        counterExtras(5, 0, IncrementRequest.COUNTER_NOT_EXISTS_EXPIRY), null));
  }

  @Test
  void getAndTouchGetAndLockAndGetMeta() throws IOException {
    GetAndTouchRequest getAndTouch = new GetAndTouchRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, EXPIRY, null);
    assertEncoding(getAndTouch, COLLECTIONS, reference(getAndTouch, COLLECTIONS, Opcode.GET_AND_TOUCH, 0, 0,
      NO_DURABILITY, false, Unpooled.buffer().writeInt((int) EXPIRY), null));

    GetAndLockRequest getAndLock = new GetAndLockRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, Duration.ofMillis(2500), null);
    assertEncoding(getAndLock, COLLECTIONS, reference(getAndLock, COLLECTIONS, Opcode.GET_AND_LOCK, 0, 0,
      NO_DURABILITY, false, Unpooled.buffer().writeInt(3), null)); // seconds, rounded up

    GetMetaRequest getMeta = new GetMetaRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, null);
    assertEncoding(getMeta, COLLECTIONS, reference(getMeta, COLLECTIONS, Opcode.GET_META, 0, 0,
      NO_DURABILITY, false, Unpooled.buffer().writeByte(2), null));
  }

  @Test
  void rejectsDurabilityAndPreserveExpiryTheServerLacks() {
    InsertRequest insert = insert(CONTENT, MAJORITY);
    assertThrows(DurabilityLevelNotAvailableException.class, () -> insert.encodeTo(newWriter(new ByteArrayOutputStream(), COLLECTIONS)));
    assertThrows(DurabilityLevelNotAvailableException.class, () -> KeyValueRequestTesting.encode(insert, COLLECTIONS));

    ReplaceRequest replace = replace(CONTENT, true, NO_DURABILITY);
    assertThrows(FeatureNotAvailableException.class, () -> replace.encodeTo(newWriter(new ByteArrayOutputStream(), COLLECTIONS)));
    assertThrows(FeatureNotAvailableException.class, () -> KeyValueRequestTesting.encode(replace, COLLECTIONS));
  }

  // Decoding

  @Test
  void decodesMutationsWithTokens() {
    byte[] packet = response(Opcode.ADD, (byte) 0, (short) 0, 555, token(), new byte[0], new byte[0]);
    KeyValueChannelContext tokens = ctx(null, EnumSet.of(ServerFeature.MUTATION_SEQNO));

    InsertRequest insert = insert(CONTENT, NO_DURABILITY);
    insert.partition(PARTITION);
    for (InsertResponse response : decodeEveryWay(insert, packet, tokens)) {
      assertEquals(ResponseStatus.SUCCESS, response.status());
      assertEquals(555, response.cas());
      assertEquals(99, response.mutationToken().orElseThrow(AssertionError::new).sequenceNumber());
    }
    for (ReplaceResponse response : decodeEveryWay(replace(CONTENT, false, NO_DURABILITY), packet, tokens)) {
      assertEquals(555, response.cas());
      assertTrue(response.mutationToken().isPresent());
    }
    for (RemoveResponse response : decodeEveryWay(remove(NO_DURABILITY), packet, tokens)) {
      assertTrue(response.mutationToken().isPresent());
    }
    for (AppendResponse response : decodeEveryWay(append(CONTENT, NO_DURABILITY), packet, tokens)) {
      assertTrue(response.mutationToken().isPresent());
    }
    for (PrependResponse response : decodeEveryWay(prepend(CONTENT, NO_DURABILITY), packet, tokens)) {
      assertTrue(response.mutationToken().isPresent());
    }
    TouchRequest touch = new TouchRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, EXPIRY, null);
    for (TouchResponse response : decodeEveryWay(touch, packet, tokens)) {
      assertTrue(response.mutationToken().isPresent());
    }

    // Without tokens enabled, none.
    for (InsertResponse response : decodeEveryWay(insert, packet)) {
      assertFalse(response.mutationToken().isPresent());
    }
  }

  @Test
  void decodesUnlock() {
    UnlockRequest unlock = new UnlockRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, CAS, null);
    byte[] packet = response(Opcode.UNLOCK, (byte) 0, MemcacheProtocol.Status.LOCKED.status(), 7, new byte[0], new byte[0], new byte[0]);
    for (UnlockResponse response : decodeEveryWay(unlock, packet)) {
      assertEquals(ResponseStatus.LOCKED, response.status());
      assertEquals(7, response.cas());
      assertFalse(response.mutationToken().isPresent());
    }
  }

  @Test
  void decodesCounterValue() {
    byte[] body = new byte[8];
    Unpooled.wrappedBuffer(body).setLong(0, 42);
    byte[] found = response(Opcode.INCREMENT, (byte) 0, (short) 0, 1, new byte[0], new byte[0], body);
    byte[] notFound = response(Opcode.INCREMENT, (byte) 0, MemcacheProtocol.Status.NOT_FOUND.status(), 0, new byte[0], new byte[0], new byte[0]);

    for (IncrementResponse response : decodeEveryWay(increment(Optional.empty(), NO_DURABILITY), found)) {
      assertEquals(42, response.value());
    }
    for (IncrementResponse response : decodeEveryWay(increment(Optional.empty(), NO_DURABILITY), notFound)) {
      assertEquals(ResponseStatus.NOT_FOUND, response.status());
      assertEquals(0, response.value());
    }
    for (DecrementResponse response : decodeEveryWay(decrement(Optional.empty(), NO_DURABILITY), found)) {
      assertEquals(42, response.value());
    }
  }

  @Test
  void decodesGetAndTouchAndGetAndLock() {
    byte[] compressed = SnappyCodec.instance().compress(LONG_CONTENT);
    byte datatype = (byte) (MemcacheProtocol.Datatype.JSON.datatype() | MemcacheProtocol.Datatype.SNAPPY.datatype());
    byte[] packet = response(Opcode.GET_AND_TOUCH, datatype, (short) 0, 9, ints(FLAGS), new byte[0], compressed);
    byte[] notFound = response(Opcode.GET_AND_TOUCH, (byte) 0, MemcacheProtocol.Status.NOT_FOUND.status(), 0, new byte[0], new byte[0], new byte[0]);

    GetAndTouchRequest getAndTouch = new GetAndTouchRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, EXPIRY, null);
    for (GetAndTouchResponse response : decodeEveryWay(getAndTouch, packet)) {
      assertArrayEquals(LONG_CONTENT, response.content());
      assertEquals(FLAGS, response.flags());
      assertEquals(9, response.cas());
    }
    for (GetAndTouchResponse response : decodeEveryWay(getAndTouch, notFound)) {
      assertNull(response.content());
    }

    GetAndLockRequest getAndLock = new GetAndLockRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, Duration.ofSeconds(3), null);
    for (GetAndLockResponse response : decodeEveryWay(getAndLock, packet)) {
      assertArrayEquals(LONG_CONTENT, response.content());
      assertEquals(FLAGS, response.flags());
    }
  }

  @Test
  void decodesGetMetaDeletedFlag() {
    GetMetaRequest getMeta = new GetMetaRequest(KEY, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, null);
    byte[] deleted = response(Opcode.GET_META, (byte) 0, (short) 0, 3, ints(1), new byte[0], new byte[0]);
    byte[] alive = response(Opcode.GET_META, (byte) 0, (short) 0, 3, ints(0), new byte[0], new byte[0]);
    for (GetMetaResponse response : decodeEveryWay(getMeta, deleted)) {
      assertTrue(response.deleted());
      assertEquals(3, response.cas());
    }
    for (GetMetaResponse response : decodeEveryWay(getMeta, alive)) {
      assertFalse(response.deleted());
    }
  }

  // Helpers

  private static InsertRequest insert(
    byte[] content,
    Optional<DurabilityLevel> durability
  ) {
    return new InsertRequest(KEY, content, EXPIRY, FLAGS, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, durability, null);
  }

  private static ReplaceRequest replace(
    byte[] content,
    boolean preserveExpiry,
    Optional<DurabilityLevel> durability
  ) {
    // With preserveExpiry, the expiry is 0 (as when it's used).
    return new ReplaceRequest(KEY, content, preserveExpiry ? 0 : EXPIRY, preserveExpiry, FLAGS, TIMEOUT, CAS,
      CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, durability, null);
  }

  private static AppendRequest append(
    byte[] content,
    Optional<DurabilityLevel> durability
  ) {
    return new AppendRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, content,
      CAS, durability, null);
  }

  private static PrependRequest prepend(
    byte[] content,
    Optional<DurabilityLevel> durability
  ) {
    return new PrependRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, content,
      CAS, durability, null);
  }

  private static RemoveRequest remove(Optional<DurabilityLevel> durability) {
    return new RemoveRequest(KEY, CAS, TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE,
      durability, null);
  }

  private static IncrementRequest increment(
    Optional<Long> initial,
    Optional<DurabilityLevel> durability
  ) {
    return new IncrementRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, 5,
      initial, EXPIRY, durability, null);
  }

  private static DecrementRequest decrement(
    Optional<Long> initial,
    Optional<DurabilityLevel> durability
  ) {
    return new DecrementRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, KEY, 5,
      initial, EXPIRY, durability, null);
  }

  private static ByteBuf flagsAndExpiry() {
    return Unpooled.buffer().writeInt(FLAGS).writeInt((int) EXPIRY);
  }

  private static ByteBuf counterExtras(
    long delta,
    long initial,
    int expiry
  ) {
    return Unpooled.buffer().writeLong(delta).writeLong(initial).writeInt(expiry);
  }

  private static byte[] token() {
    byte[] extras = new byte[16];
    Unpooled.wrappedBuffer(extras).setLong(0, 0xabcdefL).setLong(8, 99);
    return extras;
  }

  /**
   * The packet the request's old ByteBuf encoder wrote (built the same way it built it).
   */
  private static byte[] reference(
    BaseKeyValueRequest<?> request,
    KeyValueChannelContext ctx,
    Opcode opcode,
    int datatype,
    long cas,
    Optional<DurabilityLevel> durability,
    boolean preserveExpiry,
    @Nullable ByteBuf extras,
    byte @Nullable [] value
  ) {
    ByteBuf framingExtras = mutationFlexibleExtras(request, ctx, ALLOC, durability, preserveExpiry);
    ByteBuf key = Unpooled.wrappedBuffer(request.collectionPrefix(ctx), request.key());
    ByteBuf extrasOrNone = extras == null ? Unpooled.EMPTY_BUFFER : extras;
    ByteBuf body = value == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(value);
    try {
      return bytes(MemcacheProtocol.flexibleRequest(ALLOC, opcode, (byte) datatype, PARTITION, OPAQUE, cas,
        framingExtras, extrasOrNone, key, body));
    } finally {
      framingExtras.release();
      key.release();
    }
  }

  /**
   * Like {@link #reference}, for a value the old encoder compressed.
   */
  private static byte[] referenceCompressed(
    BaseKeyValueRequest<?> request,
    KeyValueChannelContext ctx,
    Opcode opcode,
    long cas,
    Optional<DurabilityLevel> durability,
    boolean preserveExpiry,
    @Nullable ByteBuf extras,
    byte[] value
  ) {
    CompressionConfig config = ctx.compressionConfig();
    ByteBuf compressed = MemcacheProtocol.tryCompression(value, config.minRatio());
    assertTrue(compressed != null, "the test value should compress");
    byte[] compressedBytes = bytes(compressed);
    return reference(request, ctx, opcode, MemcacheProtocol.Datatype.SNAPPY.datatype(), cas, durability,
      preserveExpiry, extras, compressedBytes);
  }
}
