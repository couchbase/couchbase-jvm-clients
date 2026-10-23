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
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.compression.snappy.SnappyCodec;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.deps.io.netty.buffer.CompositeByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.env.CompressionConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.subdoc.XattrUnknownVirtualAttributeException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestWriter;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.NettyKeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.mutationFlexibleExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.test.Util.readResource;
import static java.util.Collections.singletonList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the requests that implement {@link PortableKeyValueRequest}:
 * <ul>
 *   <li>Both encoders (the socket connection's {@link KeyValueRequestWriter}, and the Netty implementation's
 *   {@link NettyKeyValueRequestEncoder}) write the same packet as the request's old
 *   {@code ByteBuf} encoder (kept here, as the reference).
 *   <li>Decoding from a {@link KeyValuePacket} (directly, or through the {@code ByteBuf} bridge, from a heap
 *   or direct buffer) gives the expected response.
 * </ul>
 */
class PortableKeyValueRequestTest {
  private static final Duration TIMEOUT = Duration.ofMillis(2500);
  private static final CoreContext CORE_CONTEXT = mock(CoreContext.class);
  private static final CollectionIdentifier DEFAULT_COLLECTION = CollectionIdentifier.fromDefault("b");
  private static final CollectionIdentifier CUSTOM_COLLECTION = new CollectionIdentifier("b", Optional.of("s"), Optional.of("c"));
  private static final UnpooledByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
  private static final int OPAQUE = 0x12345678;
  private static final NettyKeyValueRequestEncoder NETTY_ENCODER = new NettyKeyValueRequestEncoder(ALLOC);
  private static final short PARTITION = 42;
  private static final int UPSERT_FLAGS = 0x02000006;
  private static final long UPSERT_EXPIRY = 3600;
  private static final byte ACCESS_DELETED = 0x04;

  private static final byte[] SHORT_CONTENT = "{\"hello\":\"world\"}".getBytes(UTF_8);
  private static final byte[] LONG_CONTENT = readResource("dummy.json", PortableKeyValueRequestTest.class).getBytes(UTF_8);
  private static final byte[] HUGE_CONTENT = new byte[200 * 1024];

  static {
    Arrays.fill(HUGE_CONTENT, (byte) 'x');
  }

  // Encoding

  @Test
  void getWithoutCollections() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.noneOf(ServerFeature.class));
    GetRequest request = get(DEFAULT_COLLECTION);
    assertEncoding(request, ctx, referenceGet(request, ctx));
  }

  @Test
  void getWithCollection() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    GetRequest request = get(CUSTOM_COLLECTION);
    assertEncoding(request, ctx, referenceGet(request, ctx));
  }

  @Test
  void replicaGet() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    ReplicaGetRequest request = new ReplicaGetRequest("some-key", TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, (short) 1, null);
    ByteBuf key = request.encodedKeyWithCollection(ALLOC, ctx);
    byte[] expected = bytes(MemcacheProtocol.request(ALLOC, MemcacheProtocol.Opcode.GET_REPLICA, noDatatype(),
      PARTITION, OPAQUE, noCas(), noExtras(), key, noBody()));
    key.release();
    assertEncoding(request, ctx, expected);
  }

  @Test
  void replicaSubdocGetSetsReplicaFlag() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    Spec spec = new Spec(SubdocCommandType.GET, "foo", false, false);
    ReplicaSubdocGetRequest request = ReplicaSubdocGetRequest.create(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, "some-key", (byte) 0,
      singletonList(new CoreSubdocGetCommand(spec.type, spec.path, spec.xattr)),
      (short) 1, null);
    assertEncoding(request, ctx, referenceSubdocGet(request, (byte) 0x20, new Spec[]{spec}, ctx));
  }

  @Test
  void getFailsWhenCollectionUnknown() {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    GetRequest request = new GetRequest("key", TIMEOUT, CORE_CONTEXT,
      new CollectionIdentifier("b", Optional.of("s"), Optional.of("unknown")), BestEffortRetryStrategy.INSTANCE, null);

    assertThrows(CollectionNotFoundException.class, () -> request.encodeTo(newWriter(new ByteArrayOutputStream()), 1, ctx));
    assertThrows(CollectionNotFoundException.class, () -> request.encode(ALLOC, 1, ctx));
  }

  @Test
  void upsertPlain() throws IOException {
    assertUpsertEncoding(SHORT_CONTENT, false, Optional.empty(), ctx(null, EnumSet.of(ServerFeature.COLLECTIONS)));
  }

  @Test
  void upsertWithCollectionDurabilityAndPreserveExpiry() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS, ServerFeature.SYNC_REPLICATION, ServerFeature.ALT_REQUEST, ServerFeature.PRESERVE_TTL));
    assertUpsertEncoding(SHORT_CONTENT, true, Optional.of(DurabilityLevel.MAJORITY), ctx);
    assertUpsertEncoding(SHORT_CONTENT, false, Optional.of(DurabilityLevel.PERSIST_TO_MAJORITY), ctx);
    assertUpsertEncoding(SHORT_CONTENT, true, Optional.of(DurabilityLevel.NONE), ctx);
  }

  @Test
  void upsertCompressed() throws IOException {
    assertUpsertEncoding(LONG_CONTENT, false, Optional.empty(), compressingCtx());
  }

  @Test
  void upsertCompressedHugeValue() throws IOException {
    assertUpsertEncoding(HUGE_CONTENT, false, Optional.empty(), compressingCtx());
  }

  @Test
  void upsertIncompressible() throws IOException {
    byte[] random = new byte[4096];
    new Random(1).nextBytes(random);
    assertUpsertEncoding(random, false, Optional.empty(), compressingCtx());
  }

  @Test
  void upsertHugeValue() throws IOException {
    assertUpsertEncoding(HUGE_CONTENT, false, Optional.empty(), ctx(null, EnumSet.of(ServerFeature.COLLECTIONS)));
  }

  @Test
  void severalCompressedUpsertsInARow() throws IOException {
    KeyValueChannelContext ctx = compressingCtx();
    byte[] other = new String(LONG_CONTENT, UTF_8).replace('a', 'z').getBytes(UTF_8);
    List<byte[]> contents = Arrays.asList(LONG_CONTENT, other, SHORT_CONTENT, LONG_CONTENT);

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    KeyValueRequestWriter writer = newWriter(out);
    for (byte[] content : contents) {
      UpsertRequest request = upsert(content, false, Optional.empty());
      request.partition(PARTITION);
      expected.write(referenceUpsert(request, content, false, Optional.empty(), ctx));

      writer.makeRoomForRequest();
      request.encodeTo(writer, OPAQUE, ctx);
      writer.completeRequestHead();
      writer.finishRequest();
    }
    writer.flush();

    assertArrayEquals(expected.toByteArray(), out.toByteArray());
  }

  @Test
  void upsertFailsWhenPreserveExpiryUnsupported() {
    UpsertRequest request = upsert(SHORT_CONTENT, true, Optional.empty());
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));

    assertThrows(FeatureNotAvailableException.class, () -> request.encodeTo(newWriter(new ByteArrayOutputStream()), 1, ctx));
    assertThrows(FeatureNotAvailableException.class, () -> request.encode(ALLOC, 1, ctx));
  }

  @Test
  void subdocGetOneCommand() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    assertSubdocGetEncoding(ctx, (byte) 0, new Spec(SubdocCommandType.GET, "foo", false, false));
  }

  @Test
  void subdocGetSeveralCommandsWithFlagsXattrsAndBinary() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS, ServerFeature.SUBDOC_BINARY_XATTR, ServerFeature.VATTR));
    assertSubdocGetEncoding(ctx, ACCESS_DELETED,
      new Spec(SubdocCommandType.GET, "_txn", true, true),
      new Spec(SubdocCommandType.GET, "$document.exptime", true, false),
      new Spec(SubdocCommandType.EXISTS, "a.b[3].c", false, false),
      new Spec(SubdocCommandType.COUNT, "ünïcödé", false, false),
      new Spec(SubdocCommandType.GET_DOC, "", false, false)
    );
  }

  @Test
  void subdocGetBinaryFlagOnlyWhenSupported() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    assertSubdocGetEncoding(ctx, (byte) 0, new Spec(SubdocCommandType.GET, "_txn", true, true));
  }

  @Test
  void subdocGetBodyBiggerThanWriteBuffer() throws IOException {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    char[] longPath = new char[60_000];
    Arrays.fill(longPath, 'p');
    assertSubdocGetEncoding(ctx, (byte) 0,
      new Spec(SubdocCommandType.GET, new String(longPath), false, false),
      new Spec(SubdocCommandType.GET, new String(longPath), false, false));
  }

  @Test
  void subdocGetRejectsUnknownVattrWhenVattrsUnsupported() {
    KeyValueChannelContext ctx = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
    SubdocGetRequest request = subdocGet((byte) 0, new Spec(SubdocCommandType.GET, "$bogus", true, false));

    assertThrows(XattrUnknownVirtualAttributeException.class, () -> request.encodeTo(newWriter(new ByteArrayOutputStream()), 1, ctx));
    assertThrows(XattrUnknownVirtualAttributeException.class, () -> request.encode(ALLOC, 1, ctx));
  }

  // Decoding

  @Test
  void decodesGet() {
    GetRequest request = get(DEFAULT_COLLECTION);
    byte[] packet = response(MemcacheProtocol.Opcode.GET, MemcacheProtocol.Datatype.JSON.datatype(), (short) 0, 1234,
      ints(0x02000006), new byte[0], SHORT_CONTENT);

    for (GetResponse response : decodeEveryWay(request, packet)) {
      assertEquals(ResponseStatus.SUCCESS, response.status());
      assertArrayEquals(SHORT_CONTENT, response.content());
      assertEquals(0x02000006, response.flags());
      assertEquals(1234, response.cas());
    }
  }

  @Test
  void decodesCompressedGet() {
    GetRequest request = get(DEFAULT_COLLECTION);
    byte[] compressed = SnappyCodec.instance().compress(LONG_CONTENT);
    byte datatype = (byte) (MemcacheProtocol.Datatype.JSON.datatype() | MemcacheProtocol.Datatype.SNAPPY.datatype());
    byte[] packet = response(MemcacheProtocol.Opcode.GET, datatype, (short) 0, 1, ints(7), new byte[0], compressed);

    for (GetResponse response : decodeEveryWay(request, packet)) {
      assertArrayEquals(LONG_CONTENT, response.content());
      assertEquals(7, response.flags());
    }
  }

  @Test
  void decodesGetNotFound() {
    GetRequest request = get(DEFAULT_COLLECTION);
    byte[] packet = response(MemcacheProtocol.Opcode.GET, (byte) 0, MemcacheProtocol.Status.NOT_FOUND.status(), 0,
      new byte[0], new byte[0], "Not found".getBytes(UTF_8));

    for (GetResponse response : decodeEveryWay(request, packet)) {
      assertEquals(ResponseStatus.NOT_FOUND, response.status());
      assertNull(response.content());
    }
  }

  @Test
  void decodesUpsertWithMutationToken() {
    UpsertRequest request = upsert(SHORT_CONTENT, false, Optional.empty());
    request.partition(PARTITION);
    byte[] extras = new byte[16];
    Unpooled.wrappedBuffer(extras).setLong(0, 0xabcdefL).setLong(8, 99);
    byte[] packet = response(MemcacheProtocol.Opcode.SET, (byte) 0, (short) 0, 555, extras, new byte[0], new byte[0]);

    KeyValueChannelContext tokens = ctx(null, EnumSet.of(ServerFeature.MUTATION_SEQNO));
    for (UpsertResponse response : decodeEveryWay(request, packet, tokens)) {
      assertEquals(ResponseStatus.SUCCESS, response.status());
      assertEquals(555, response.cas());
      MutationToken token = response.mutationToken().orElseThrow(AssertionError::new);
      assertEquals(PARTITION, token.partitionID());
      assertEquals(0xabcdefL, token.partitionUUID());
      assertEquals(99, token.sequenceNumber());
      assertEquals("b", token.bucketName());
    }

    for (UpsertResponse response : decodeEveryWay(request, packet)) {
      assertFalse(response.mutationToken().isPresent());
    }
  }

  @Test
  void decodesSubdocGetInOriginalOrderWithFieldErrors() {
    List<SubdocGetRequest.Command> commands = SubdocGetRequest.convertCommands(Arrays.asList(
      new CoreSubdocGetCommand(SubdocCommandType.GET, "a", false),
      new CoreSubdocGetCommand(SubdocCommandType.GET, "missing", false),
      new CoreSubdocGetCommand(SubdocCommandType.GET, "x", true)
    ));
    SubdocGetRequest request = new SubdocGetRequest(TIMEOUT, CORE_CONTEXT, DEFAULT_COLLECTION,
      BestEffortRetryStrategy.INSTANCE, "some-key", (byte) 0, commands, null);

    // Results in wire order: xattr "x" first, then "a", then "missing".
    ByteBuf body = Unpooled.buffer()
      .writeShort(0).writeInt(3).writeBytes("\"x\"".getBytes(UTF_8))
      .writeShort(0).writeInt(1).writeBytes("1".getBytes(UTF_8))
      .writeShort(MemcacheProtocol.Status.SUBDOC_PATH_NOT_FOUND.status()).writeInt(0);
    byte[] bodyBytes = ByteBufUtil.getBytes(body);

    byte[] plain = response(MemcacheProtocol.Opcode.SUBDOC_MULTI_LOOKUP, (byte) 0,
      MemcacheProtocol.Status.SUBDOC_MULTI_PATH_FAILURE.status(), 77, new byte[0], new byte[0], bodyBytes);
    byte[] compressed = response(MemcacheProtocol.Opcode.SUBDOC_MULTI_LOOKUP, MemcacheProtocol.Datatype.SNAPPY.datatype(),
      MemcacheProtocol.Status.SUBDOC_MULTI_PATH_FAILURE.status(), 77, new byte[0], new byte[0], SnappyCodec.instance().compress(bodyBytes));

    for (byte[] packet : Arrays.asList(plain, compressed)) {
      for (SubdocGetResponse response : decodeEveryWay(request, packet)) {
        assertEquals(ResponseStatus.SUCCESS, response.status());
        assertEquals(77, response.cas());
        assertFalse(response.error().isPresent());
        SubDocumentField[] values = response.values();
        assertEquals("a", values[0].path());
        assertArrayEquals("1".getBytes(UTF_8), values[0].value());
        assertEquals(SubDocumentOpResponseStatus.PATH_NOT_FOUND, values[1].status());
        assertTrue(values[1].error().isPresent());
        assertEquals(0, values[1].value().length);
        assertArrayEquals("\"x\"".getBytes(UTF_8), values[2].value());
      }
    }
  }

  @Test
  void decodesSubdocGetErrorMessage() {
    SubdocGetRequest request = subdocGet((byte) 0, new Spec(SubdocCommandType.GET, "a", false, false));
    byte[] packet = response(MemcacheProtocol.Opcode.SUBDOC_MULTI_LOOKUP, MemcacheProtocol.Datatype.JSON.datatype(),
      MemcacheProtocol.Status.INVALID_REQUEST.status(), 0, new byte[0], new byte[0],
      "{\"error\":{\"context\":\"bad path\"}}".getBytes(UTF_8));

    for (SubdocGetResponse response : decodeEveryWay(request, packet)) {
      assertEquals(0, response.values().length);
      String message = response.error().orElseThrow(AssertionError::new).getMessage();
      assertTrue(message.contains("bad path"), message);
    }
  }

  // Helpers

  private static void assertUpsertEncoding(byte[] content, boolean preserveExpiry, Optional<DurabilityLevel> durability,
                                           KeyValueChannelContext ctx) throws IOException {
    UpsertRequest request = upsert(content, preserveExpiry, durability);
    assertEncoding(request, ctx, referenceUpsert(request, content, preserveExpiry, durability, ctx));
  }

  private static void assertSubdocGetEncoding(KeyValueChannelContext ctx, byte flags, Spec... specs) throws IOException {
    SubdocGetRequest request = subdocGet(flags, specs);
    assertEncoding(request, ctx, referenceSubdocGet(request, flags, specs, ctx));
  }

  private static void assertEncoding(PortableKeyValueRequest<?> request, KeyValueChannelContext ctx, byte[] expected) throws IOException {
    request.partition(PARTITION);

    // The socket connection's writer.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    KeyValueRequestWriter writer = newWriter(out);
    writer.makeRoomForRequest();
    request.encodeTo(writer, OPAQUE, ctx);
    writer.completeRequestHead();
    assertArrayEquals(expected, writer.requestBytes());
    writer.finishRequest();
    writer.flush();
    assertArrayEquals(expected, out.toByteArray());

    // The Netty implementation's encoder: the channel's (reused by every test), and a new one (the ByteBuf bridge).
    for (ByteBuf encoded : Arrays.asList(NETTY_ENCODER.encode(request, OPAQUE, ctx), request.encode(ALLOC, OPAQUE, ctx))) {
      try {
        assertTrue(MemcacheProtocol.verifyRequest(encoded));
        assertArrayEquals(expected, ByteBufUtil.getBytes(encoded));
      } finally {
        encoded.release();
      }
    }
  }

  /**
   * Decodes the response directly from a packet, and through the ByteBuf bridge from a heap buffer
   * (like the socket connection's), from direct buffers (like Netty's), and from a composite buffer.
   */
  private static <R extends Response> List<R> decodeEveryWay(
    PortableKeyValueRequest<R> request,
    byte[] packet,
    KeyValueChannelContext ctx
  ) {
    byte[] padded = new byte[packet.length + 10];
    System.arraycopy(packet, 0, padded, 3, packet.length);
    R fromPacket = request.decode(KeyValuePacket.wrap(padded, 3, packet.length), ctx);

    R fromHeap = request.decode(Unpooled.wrappedBuffer(packet), ctx);

    ByteBuf direct = Unpooled.directBuffer(packet.length).writeBytes(packet);
    ByteBuf pooled = PooledByteBufAllocator.DEFAULT.directBuffer(packet.length).writeBytes(packet);
    pooled.readerIndex(5); // ignored, like MemcacheProtocol's readers
    try {
      R fromDirect = request.decode(direct, ctx);
      R fromPooled = request.decode(pooled, ctx);
      // Several components (no single internal NIO buffer).
      int half = packet.length / 2;
      R fromComposite = request.decode(Unpooled.wrappedBuffer(
        Unpooled.wrappedBuffer(packet, 0, half), Unpooled.wrappedBuffer(packet, half, packet.length - half)), ctx);
      return Arrays.asList(fromPacket, fromHeap, fromDirect, fromPooled, fromComposite);
    } finally {
      direct.release();
      pooled.release();
    }
  }

  private static <R extends Response> List<R> decodeEveryWay(
    PortableKeyValueRequest<R> request,
    byte[] packet
  ) {
    return decodeEveryWay(request, packet, ctx(null, EnumSet.noneOf(ServerFeature.class)));
  }

  private static byte[] response(MemcacheProtocol.Opcode opcode, byte datatype, short status, long cas,
                                 byte[] extras, byte[] key, byte[] body) {
    ByteBuf packet = MemcacheProtocol.response(ALLOC, opcode, datatype, status, OPAQUE, cas,
      Unpooled.wrappedBuffer(extras), Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(body));
    try {
      return ByteBufUtil.getBytes(packet);
    } finally {
      packet.release();
    }
  }

  private static byte[] ints(int value) {
    byte[] bytes = new byte[4];
    Unpooled.wrappedBuffer(bytes).setInt(0, value);
    return bytes;
  }

  private static KeyValueRequestWriter newWriter(ByteArrayOutputStream out) {
    return new KeyValueRequestWriter(out, 64 * 1024);
  }

  private static GetRequest get(CollectionIdentifier collection) {
    return new GetRequest("some-key", TIMEOUT, CORE_CONTEXT, collection, BestEffortRetryStrategy.INSTANCE, null);
  }

  private static UpsertRequest upsert(byte[] content, boolean preserveExpiry, Optional<DurabilityLevel> durability) {
    return new UpsertRequest("some-key", content, UPSERT_EXPIRY, preserveExpiry, UPSERT_FLAGS, TIMEOUT, CORE_CONTEXT,
      CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE, durability, null);
  }

  /**
   * A sub-document lookup command (whose fields the request doesn't expose).
   */
  private static final class Spec {
    final SubdocCommandType type;
    final String path;
    final boolean xattr;
    final boolean binary;

    Spec(SubdocCommandType type, String path, boolean xattr, boolean binary) {
      this.type = type;
      this.path = path;
      this.xattr = xattr;
      this.binary = binary;
    }
  }

  private static SubdocGetRequest subdocGet(byte flags, Spec... specs) {
    List<SubdocGetRequest.Command> commands = new ArrayList<>();
    for (int i = 0; i < specs.length; i++) {
      commands.add(new SubdocGetRequest.Command(specs[i].type, specs[i].path, specs[i].xattr, specs[i].binary, i));
    }
    return new SubdocGetRequest(TIMEOUT, CORE_CONTEXT, CUSTOM_COLLECTION, BestEffortRetryStrategy.INSTANCE,
      "some-key", flags, commands, null);
  }

  private static KeyValueChannelContext compressingCtx() {
    return ctx(CompressionConfig.builder().enable(true).build(), EnumSet.of(ServerFeature.COLLECTIONS));
  }

  private static KeyValueChannelContext ctx(CompressionConfig compression, Set<ServerFeature> features) {
    CollectionMap collectionMap = new CollectionMap();
    collectionMap.put(CUSTOM_COLLECTION, new byte[]{0x09});
    return new KeyValueChannelContext(compression, Optional.of("b"), collectionMap, null, features);
  }

  // The requests' ByteBuf encoders, from before they were converted: the reference.

  private static byte[] referenceGet(GetRequest request, KeyValueChannelContext ctx) {
    ByteBuf key = request.encodedKeyWithCollection(ALLOC, ctx);
    try {
      return bytes(MemcacheProtocol.request(ALLOC, MemcacheProtocol.Opcode.GET, noDatatype(),
        PARTITION, OPAQUE, noCas(), noExtras(), key, noBody()));
    } finally {
      key.release();
    }
  }

  private static byte[] referenceUpsert(UpsertRequest request, byte[] value, boolean preserveExpiry,
                                        Optional<DurabilityLevel> durability, KeyValueChannelContext ctx) {
    ByteBuf key = null;
    ByteBuf content = null;
    ByteBuf extras = null;
    ByteBuf flexibleExtras = mutationFlexibleExtras(request, ctx, ALLOC, durability, preserveExpiry);

    try {
      key = request.encodedKeyWithCollection(ALLOC, ctx);

      byte datatype = 0;
      CompressionConfig config = ctx.compressionConfig();
      if (config != null && config.enabled() && value.length >= config.minSize()) {
        ByteBuf maybeCompressed = MemcacheProtocol.tryCompression(value, config.minRatio());
        if (maybeCompressed != null) {
          datatype |= MemcacheProtocol.Datatype.SNAPPY.datatype();
          content = maybeCompressed;
        } else {
          content = Unpooled.wrappedBuffer(value);
        }
      } else {
        content = Unpooled.wrappedBuffer(value);
      }

      extras = ALLOC.buffer(Integer.BYTES * 2);
      extras.writeInt(UPSERT_FLAGS);
      extras.writeInt((int) UPSERT_EXPIRY);

      return bytes(MemcacheProtocol.flexibleRequest(ALLOC, MemcacheProtocol.Opcode.SET, datatype,
        PARTITION, OPAQUE, noCas(), flexibleExtras, extras, key, content));
    } finally {
      ReferenceCountUtil.release(key);
      ReferenceCountUtil.release(extras);
      ReferenceCountUtil.release(flexibleExtras);
      ReferenceCountUtil.release(content);
    }
  }

  private static byte[] referenceSubdocGet(SubdocGetRequest request, byte docFlags, Spec[] specs, KeyValueChannelContext ctx) {
    ByteBuf key = request.encodedKeyWithCollection(ALLOC, ctx);
    ByteBuf extras = docFlags != 0 ? ALLOC.buffer(1).writeByte(docFlags) : noExtras();
    CompositeByteBuf body = ALLOC.compositeBuffer();
    try {
      for (Spec spec : specs) {
        byte[] path = spec.path.getBytes(UTF_8);
        byte flags = 0;
        if (spec.xattr) {
          flags |= 0x04;
        }
        if (spec.binary && ctx.subdocBinaryXattr()) {
          flags |= 0x20;
        }
        ByteBuf buffer = ALLOC.buffer(4 + path.length)
          .writeByte(spec.type.opcode())
          .writeByte(flags)
          .writeShort(path.length)
          .writeBytes(path);
        body.addComponent(true, buffer);
      }
      return bytes(MemcacheProtocol.request(ALLOC, MemcacheProtocol.Opcode.SUBDOC_MULTI_LOOKUP, noDatatype(),
        PARTITION, OPAQUE, noCas(), extras, key, body));
    } finally {
      key.release();
      extras.release();
      body.release();
    }
  }

  private static byte[] bytes(ByteBuf buf) {
    try {
      return ByteBufUtil.getBytes(buf);
    } finally {
      buf.release();
    }
  }
}
