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
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.config.ConfigVersion;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.NettyMemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.kv.CoreRangeScan;
import com.couchbase.client.core.kv.CoreRangeScanId;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.CoreSamplingScan;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.util.CbStrings;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Datatype.JSON;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.ALLOC;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.CORE_CONTEXT;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.CUSTOM_COLLECTION;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.DEFAULT_COLLECTION;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.OPAQUE;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.PARTITION;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.TIMEOUT;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.assertEncoding;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.assertEncodingAsConstructed;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.bytes;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.ctx;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.decodeEveryWay;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.newWriter;
import static com.couchbase.client.core.msg.kv.KeyValueRequestTesting.response;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

/**
 * Verifies the requests without a document key (in the key field): each encodes, with both encoders, the same
 * packet as its old {@code ByteBuf} encoder (built here with {@link NettyMemcacheProtocol#request}, as the reference),
 * and decodes its response.
 */
class NonDocumentRequestTest {
  private static final String KEY = "some-key";
  private static final short SUCCESS = MemcacheProtocol.Status.SUCCESS.status();
  private static final short NOT_FOUND = MemcacheProtocol.Status.NOT_FOUND.status();
  private static final byte[] NONE = new byte[0];

  private static final KeyValueChannelContext NO_FEATURES = ctx(null, EnumSet.noneOf(ServerFeature.class));
  private static final KeyValueChannelContext COLLECTIONS = ctx(null, EnumSet.of(ServerFeature.COLLECTIONS));
  private static final KeyValueChannelContext KNOWN_CONFIG_VERSION =
    ctx(null, EnumSet.of(ServerFeature.GET_CLUSTER_CONFIG_WITH_KNOWN_VERSION));

  /**
   * For the range scan requests, which get a span (and their timeout and retry strategy) from the context.
   */
  private static final CoreContext SCAN_CONTEXT = mock(CoreContext.class, RETURNS_DEEP_STUBS);
  private static final CoreScanOptions SCAN_OPTIONS = new TestScanOptions();
  private static final CoreRangeScanId SCAN_ID = new CoreRangeScanId("0123456789abcdef".getBytes(UTF_8));

  // Noop and ping

  @Test
  void noopAndPing() throws IOException {
    // The partition is ignored: noop always targets partition 0.
    byte[] expected = reference(Opcode.NOOP, 0, (short) 0, NONE, NONE);
    assertEncoding(noop(), NO_FEATURES, expected);
    assertEncoding(ping(), NO_FEATURES, expected);
  }

  @Test
  void decodesNoopAndPing() {
    for (NoopResponse decoded : decodeEveryWay(noop(), response(Opcode.NOOP, (byte) 0, SUCCESS, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.SUCCESS, decoded.status());
    }
    for (NoopResponse decoded : decodeEveryWay(ping(), response(Opcode.NOOP, (byte) 0, SUCCESS, 0, NONE, NONE, NONE))) {
      assertInstanceOf(KvPingResponse.class, decoded);
      assertEquals(ResponseStatus.SUCCESS, decoded.status());
    }
  }

  // Collections

  @Test
  void getCollectionManifest() throws IOException {
    GetCollectionManifestRequest request = new GetCollectionManifestRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, DEFAULT_COLLECTION);
    assertEncoding(request, NO_FEATURES, reference(Opcode.COLLECTIONS_GET_MANIFEST, 0, (short) 0, NONE, NONE));

    byte[] manifest = "{\"uid\":\"7\"}".getBytes(UTF_8);
    for (GetCollectionManifestResponse decoded : decodeEveryWay(request,
      response(Opcode.COLLECTIONS_GET_MANIFEST, (byte) 0, SUCCESS, 0, NONE, NONE, manifest))) {
      assertEquals(Optional.of("{\"uid\":\"7\"}"), decoded.manifest());
    }
    for (GetCollectionManifestResponse decoded : decodeEveryWay(request,
      response(Opcode.COLLECTIONS_GET_MANIFEST, (byte) 0, NOT_FOUND, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.NOT_FOUND, decoded.status());
      assertFalse(decoded.manifest().isPresent());
    }
  }

  @Test
  void getCollectionId() throws IOException {
    GetCollectionIdRequest request = new GetCollectionIdRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, CUSTOM_COLLECTION);
    assertEncoding(request, NO_FEATURES, reference(Opcode.COLLECTIONS_GET_CID, 0, (short) 0, NONE, "s.c".getBytes(UTF_8)));

    // The manifest UID, then the collection ID (unsigned).
    byte[] extras = bytes(Unpooled.buffer().writeLong(0x77).writeInt(0x8000_0009));
    for (GetCollectionIdResponse decoded : decodeEveryWay(request,
      response(Opcode.COLLECTIONS_GET_CID, (byte) 0, SUCCESS, 0, extras, NONE, NONE))) {
      assertEquals(Optional.of(0x8000_0009L), decoded.collectionId());
    }
    for (GetCollectionIdResponse decoded : decodeEveryWay(request,
      response(Opcode.COLLECTIONS_GET_CID, (byte) 0, NOT_FOUND, 0, NONE, NONE, NONE))) {
      assertFalse(decoded.collectionId().isPresent());
    }
    assertThrowsExactly(RuntimeException.class, () -> decodeEveryWay(request,
      response(Opcode.COLLECTIONS_GET_CID, (byte) 0, SUCCESS, 0, new byte[8], NONE, NONE)));
  }

  @Test
  void getCollectionIdNeedsACollectionName() {
    CollectionIdentifier scopeOnly = new CollectionIdentifier("b", Optional.of("s"), Optional.empty());
    GetCollectionIdRequest request = new GetCollectionIdRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, scopeOnly);
    assertThrows(InvalidArgumentException.class, () -> request.encodeTo(newWriter(new ByteArrayOutputStream(), NO_FEATURES)));
  }

  // Observe

  @Test
  void observeViaSeqno() throws IOException {
    ObserveViaSeqnoRequest request = seqnoRequest();
    assertEncoding(request, NO_FEATURES,
      reference(Opcode.OBSERVE_SEQ, 0, PARTITION, NONE, bytes(Unpooled.buffer().writeLong(0x1234_5678_9abcL))));
  }

  @Test
  void decodesObserveViaSeqno() {
    byte[] format0 = bytes(Unpooled.buffer().writeByte(0).writeShort(PARTITION).writeLong(0x1234).writeLong(5).writeLong(6));
    for (ObserveViaSeqnoResponse decoded : decodeEveryWay(seqnoRequest(),
      response(Opcode.OBSERVE_SEQ, (byte) 0, SUCCESS, 0, NONE, NONE, format0))) {
      assertEquals(PARTITION, decoded.vbucketID());
      assertEquals(0x1234, decoded.vbucketUUID());
      assertEquals(5, decoded.lastPersistedSeqNo());
      assertEquals(6, decoded.currentSeqNo());
      assertFalse(decoded.oldVbucketUUID().isPresent());
    }

    byte[] format1 = bytes(Unpooled.buffer().writeByte(1).writeShort(PARTITION).writeLong(0x1234).writeLong(5).writeLong(6)
      .writeLong(0x4321).writeLong(4));
    for (ObserveViaSeqnoResponse decoded : decodeEveryWay(seqnoRequest(),
      response(Opcode.OBSERVE_SEQ, (byte) 0, SUCCESS, 0, NONE, NONE, format1))) {
      assertEquals(Optional.of(0x4321L), decoded.oldVbucketUUID());
      assertEquals(Optional.of(4L), decoded.lastSeqNoReceived());
    }

    byte[] format2 = bytes(Unpooled.buffer().writeByte(2).writeShort(PARTITION).writeLong(0x1234).writeLong(5).writeLong(6));
    assertThrowsExactly(RuntimeException.class, () -> decodeEveryWay(seqnoRequest(),
      response(Opcode.OBSERVE_SEQ, (byte) 0, SUCCESS, 0, NONE, NONE, format2)));
  }

  @Test
  void observeViaCas() throws IOException {
    byte[] key = KEY.getBytes(UTF_8);
    assertEncoding(observeViaCas(CUSTOM_COLLECTION), COLLECTIONS, reference(Opcode.OBSERVE_CAS, 0, PARTITION, NONE,
      bytes(Unpooled.buffer().writeShort(PARTITION).writeShort(1 + key.length).writeByte(0x09).writeBytes(key))));
    assertEncoding(observeViaCas(DEFAULT_COLLECTION), NO_FEATURES, reference(Opcode.OBSERVE_CAS, 0, PARTITION, NONE,
      bytes(Unpooled.buffer().writeShort(PARTITION).writeShort(key.length).writeBytes(key))));
    assertThrows(CollectionNotFoundException.class,
      () -> observeViaCas(new CollectionIdentifier("b", Optional.of("s"), Optional.of("unknown")))
        .encodeTo(newWriter(new ByteArrayOutputStream(), COLLECTIONS)));
  }

  @Test
  void decodesObserveViaCas() {
    byte[] key = KEY.getBytes(UTF_8);
    byte[] body = bytes(Unpooled.buffer().writeShort(PARTITION).writeShort(key.length).writeBytes(key)
      .writeByte(ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED.value()).writeLong(0xcafe));
    for (ObserveViaCasResponse decoded : decodeEveryWay(observeViaCas(DEFAULT_COLLECTION),
      response(Opcode.OBSERVE_CAS, (byte) 0, SUCCESS, 0, NONE, NONE, body))) {
      assertEquals(ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED, decoded.observeStatus());
      assertEquals(0xcafe, decoded.cas());
    }
  }

  @Test
  void multiObserveViaCas() throws IOException {
    byte[] first = "first".getBytes(UTF_8);
    byte[] second = "second-key".getBytes(UTF_8);
    Map<byte[], Short> keys = new LinkedHashMap<>();
    keys.put(first, (short) 3);
    keys.put(second, (short) 700);

    assertEncoding(multiObserveViaCas(CUSTOM_COLLECTION, keys), COLLECTIONS, reference(Opcode.OBSERVE_CAS, 0, PARTITION, NONE,
      bytes(Unpooled.buffer()
        .writeShort(3).writeShort(1 + first.length).writeByte(0x09).writeBytes(first)
        .writeShort(700).writeShort(1 + second.length).writeByte(0x09).writeBytes(second))));
    assertEncoding(multiObserveViaCas(DEFAULT_COLLECTION, keys), NO_FEATURES, reference(Opcode.OBSERVE_CAS, 0, PARTITION, NONE,
      bytes(Unpooled.buffer()
        .writeShort(3).writeShort(first.length).writeBytes(first)
        .writeShort(700).writeShort(second.length).writeBytes(second))));
  }

  @Test
  void decodesMultiObserveViaCas() {
    byte[] first = "first".getBytes(UTF_8);
    byte[] second = "second-key".getBytes(UTF_8);
    byte found = ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED.value();
    byte notFound = ObserveViaCasResponse.ObserveStatus.NOT_FOUND_PERSISTED.value();
    // Each key has a two-byte collection prefix (a collection ID of 128), which the decoder removes.
    byte[] body = bytes(Unpooled.buffer()
      .writeShort(3).writeShort(2 + first.length).writeByte(0x80).writeByte(0x01).writeBytes(first)
      .writeByte(found).writeLong(1)
      .writeShort(700).writeShort(2 + second.length).writeByte(0x80).writeByte(0x01).writeBytes(second)
      .writeByte(notFound).writeLong(2));
    MultiObserveViaCasRequest request = multiObserveViaCas(CUSTOM_COLLECTION, Collections.emptyMap());

    List<MultiObserveViaCasResponse> decodedEveryWay = decodeEveryWay(request,
      response(Opcode.OBSERVE_CAS, (byte) 0, SUCCESS, 0, NONE, NONE, body), COLLECTIONS);
    for (MultiObserveViaCasResponse decoded : decodedEveryWay) {
      // Only the keys the predicate accepts (found).
      assertEquals(1, decoded.observed().size());
      Map.Entry<byte[], ObserveViaCasResponse.ObserveStatus> entry = decoded.observed().entrySet().iterator().next();
      assertArrayEquals(first, entry.getKey());
      assertEquals(ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED, entry.getValue());
    }
  }

  // Config

  @Test
  void carrierConfigs() throws IOException {
    ConfigVersion version = new ConfigVersion(3, 7);
    byte[] versionExtras = bytes(Unpooled.buffer().writeLong(3).writeLong(7));

    assertEncoding(globalConfig(version), KNOWN_CONFIG_VERSION, reference(Opcode.GET_CONFIG, 0, (short) 0, versionExtras, NONE));
    assertEncoding(bucketConfig(version), KNOWN_CONFIG_VERSION, reference(Opcode.GET_CONFIG, 0, (short) 0, versionExtras, NONE));

    // No version if the server doesn't support it, or the client doesn't have one.
    byte[] unversioned = reference(Opcode.GET_CONFIG, 0, (short) 0, NONE, NONE);
    assertEncoding(globalConfig(version), NO_FEATURES, unversioned);
    assertEncoding(bucketConfig(version), NO_FEATURES, unversioned);
    assertEncoding(globalConfig(null), KNOWN_CONFIG_VERSION, unversioned);
    assertEncoding(bucketConfig(null), KNOWN_CONFIG_VERSION, unversioned);
  }

  @Test
  void decodesCarrierConfigs() {
    byte[] config = "{\"rev\":7}".getBytes(UTF_8);
    for (CarrierGlobalConfigResponse decoded : decodeEveryWay(globalConfig(null),
      response(Opcode.GET_CONFIG, (byte) 0, SUCCESS, 0, NONE, NONE, config))) {
      assertArrayEquals(config, decoded.content());
    }
    for (CarrierBucketConfigResponse decoded : decodeEveryWay(bucketConfig(null),
      response(Opcode.GET_CONFIG, (byte) 0, SUCCESS, 0, NONE, NONE, config))) {
      assertArrayEquals(config, decoded.content());
    }
    for (CarrierBucketConfigResponse decoded : decodeEveryWay(bucketConfig(null),
      response(Opcode.GET_CONFIG, (byte) 0, NOT_FOUND, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.NOT_FOUND, decoded.status());
      assertArrayEquals(NONE, decoded.content());
    }
  }

  // Range scan

  @Test
  void rangeScanCreate() throws IOException {
    RangeScanCreateRequest request = RangeScanCreateRequest.forRangeScan("foo".getBytes(UTF_8), CoreRangeScan.forPrefix("foo"),
      SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, CUSTOM_COLLECTION, Collections.emptyMap());
    Map<String, Object> payload = new HashMap<>();
    payload.put("collection", "9");
    payload.put("range", mapOf(
      "start", Base64.getEncoder().encodeToString("foo".getBytes(UTF_8)),
      "excl_end", Base64.getEncoder().encodeToString(("foo" + CbStrings.MAX_CODE_POINT_AS_STRING).getBytes(UTF_8))
    ));
    assertEncodingAsConstructed(request, COLLECTIONS,
      reference(Opcode.RANGE_SCAN_CREATE, JSON.datatype(), PARTITION, NONE, Mapper.encodeAsBytes(payload)));
  }

  @Test
  void samplingScanCreate() throws IOException {
    RangeScanCreateRequest request = RangeScanCreateRequest.forSamplingScan(new TestSamplingScan(10, 5),
      SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, DEFAULT_COLLECTION, Collections.emptyMap());
    Map<String, Object> payload = new HashMap<>();
    payload.put("sampling", mapOf("samples", 10L, "seed", 5L));
    // The default collection (ID 0) isn't named.
    assertEncodingAsConstructed(request, COLLECTIONS,
      reference(Opcode.RANGE_SCAN_CREATE, JSON.datatype(), PARTITION, NONE, Mapper.encodeAsBytes(payload)));
  }

  @Test
  void rangeScanCreateNeedsAKnownCollection() {
    RangeScanCreateRequest request = RangeScanCreateRequest.forRangeScan("foo".getBytes(UTF_8), CoreRangeScan.forPrefix("foo"),
      SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, new CollectionIdentifier("b", Optional.of("s"), Optional.of("unknown")),
      Collections.emptyMap());
    assertThrows(CollectionNotFoundException.class, () -> request.encodeTo(newWriter(new ByteArrayOutputStream(), COLLECTIONS)));
  }

  @Test
  void decodesRangeScanCreate() {
    RangeScanCreateRequest request = RangeScanCreateRequest.forRangeScan("foo".getBytes(UTF_8), CoreRangeScan.forPrefix("foo"),
      SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, CUSTOM_COLLECTION, Collections.emptyMap());
    for (RangeScanCreateResponse decoded : decodeEveryWay(request,
      response(Opcode.RANGE_SCAN_CREATE, (byte) 0, SUCCESS, 0, NONE, NONE, SCAN_ID.bytes()))) {
      assertArrayEquals(SCAN_ID.bytes(), decoded.rangeScanId().bytes());
    }
    for (RangeScanCreateResponse decoded : decodeEveryWay(request,
      response(Opcode.RANGE_SCAN_CREATE, (byte) 0, NOT_FOUND, 0, NONE, NONE, NONE))) {
      assertNull(decoded.rangeScanId());
    }
  }

  @Test
  void rangeScanContinueAndCancel() throws IOException {
    RangeScanContinueRequest scanContinue = rangeScanContinue();
    assertEncodingAsConstructed(scanContinue, NO_FEATURES, reference(Opcode.RANGE_SCAN_CONTINUE, 0, PARTITION,
      bytes(Unpooled.buffer().writeBytes(SCAN_ID.bytes()).writeInt(50).writeInt((int) TIMEOUT.toMillis()).writeInt(4096)), NONE));

    RangeScanCancelRequest cancel = new RangeScanCancelRequest(SCAN_ID, SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, DEFAULT_COLLECTION);
    assertEncodingAsConstructed(cancel, NO_FEATURES, reference(Opcode.RANGE_SCAN_CANCEL, 0, PARTITION, SCAN_ID.bytes(), NONE));

    for (RangeScanContinueResponse decoded : decodeEveryWay(scanContinue,
      response(Opcode.RANGE_SCAN_CONTINUE, (byte) 0, MemcacheProtocol.Status.RANGE_SCAN_MORE.status(), 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.CONTINUE, decoded.status());
    }
    for (RangeScanCancelResponse decoded : decodeEveryWay(cancel,
      response(Opcode.RANGE_SCAN_CANCEL, (byte) 0, SUCCESS, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.SUCCESS, decoded.status());
    }
  }

  /**
   * Encodes the packet with {@link NettyMemcacheProtocol#request}, like the old {@code ByteBuf} encoders.
   */
  // Reauthentication

  @Test
  void singleStepAuth() throws IOException {
    SingleStepAuthRequest request = new SingleStepAuthRequest(CORE_CONTEXT, SingleStepSaslAuthParameters.plain(null, "user", "pass"));
    byte[] expected = bytes(NettyMemcacheProtocol.request(ALLOC, Opcode.SASL_AUTH, (byte) 0, (short) 0, OPAQUE, 0,
      Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer("PLAIN", UTF_8), Unpooled.copiedBuffer("\0user\0pass", UTF_8)));
    assertEncodingAsConstructed(request, NO_FEATURES, expected);
  }

  @Test
  void decodesSingleStepAuth() {
    SingleStepAuthRequest request = new SingleStepAuthRequest(CORE_CONTEXT, SingleStepSaslAuthParameters.plain(null, "user", "pass"));
    for (SingleStepAuthResponse decoded : decodeEveryWay(request, response(Opcode.SASL_AUTH, (byte) 0, SUCCESS, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.SUCCESS, decoded.status());
    }
    short authError = MemcacheProtocol.Status.AUTH_ERROR.status();
    for (SingleStepAuthResponse decoded : decodeEveryWay(request, response(Opcode.SASL_AUTH, (byte) 0, authError, 0, NONE, NONE, NONE))) {
      assertEquals(ResponseStatus.NO_ACCESS, decoded.status());
    }
  }

  private static byte[] reference(
    Opcode opcode,
    int datatype,
    short partition,
    byte[] extras,
    byte[] body
  ) {
    return bytes(NettyMemcacheProtocol.request(ALLOC, opcode, (byte) datatype, partition, OPAQUE, 0,
      Unpooled.wrappedBuffer(extras), Unpooled.EMPTY_BUFFER, Unpooled.wrappedBuffer(body)));
  }

  private static NoopRequest noop() {
    return new NoopRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, DEFAULT_COLLECTION);
  }

  private static KvPingRequest ping() {
    return new KvPingRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, DEFAULT_COLLECTION, null);
  }

  private static ObserveViaSeqnoRequest seqnoRequest() {
    return new ObserveViaSeqnoRequest(TIMEOUT, CORE_CONTEXT, DEFAULT_COLLECTION, BestEffortRetryStrategy.INSTANCE,
      0, true, 0x1234_5678_9abcL, KEY, null);
  }

  private static ObserveViaCasRequest observeViaCas(CollectionIdentifier collection) {
    return new ObserveViaCasRequest(TIMEOUT, CORE_CONTEXT, collection, BestEffortRetryStrategy.INSTANCE, KEY, true, 0);
  }

  private static MultiObserveViaCasRequest multiObserveViaCas(CollectionIdentifier collection, Map<byte[], Short> keys) {
    return new MultiObserveViaCasRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, collection, null, keys,
      status -> status == ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED);
  }

  private static CarrierGlobalConfigRequest globalConfig(ConfigVersion ifNewerThan) {
    return new CarrierGlobalConfigRequest(TIMEOUT, CORE_CONTEXT, BestEffortRetryStrategy.INSTANCE, null, ifNewerThan);
  }

  private static CarrierBucketConfigRequest bucketConfig(ConfigVersion ifNewerThan) {
    return new CarrierBucketConfigRequest(TIMEOUT, CORE_CONTEXT, DEFAULT_COLLECTION, BestEffortRetryStrategy.INSTANCE, null, ifNewerThan);
  }

  private static RangeScanContinueRequest rangeScanContinue() {
    Sinks.Many<CoreRangeScanItem> sink = Sinks.many().unicast().onBackpressureBuffer();
    return new RangeScanContinueRequest(SCAN_ID, sink, KEY, SCAN_OPTIONS, PARTITION, SCAN_CONTEXT, DEFAULT_COLLECTION);
  }

  private static final class TestScanOptions implements CoreScanOptions {
    @Override
    public CoreCommonOptions commonOptions() {
      return CoreCommonOptions.of(TIMEOUT, BestEffortRetryStrategy.INSTANCE, null);
    }

    @Override
    public boolean idsOnly() {
      return false;
    }

    @Override
    public CoreMutationState consistentWith() {
      return null;
    }

    @Override
    public int batchItemLimit() {
      return 50;
    }

    @Override
    public int batchByteLimit() {
      return 4096;
    }
  }

  private static final class TestSamplingScan implements CoreSamplingScan {
    private final long limit;
    private final long seed;

    TestSamplingScan(long limit, long seed) {
      this.limit = limit;
      this.seed = seed;
    }

    @Override
    public long limit() {
      return limit;
    }

    @Override
    public Optional<Long> seed() {
      return Optional.of(seed);
    }
  }
}
