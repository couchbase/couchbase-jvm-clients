/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.kv.CoreRangeScanId;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.UnsignedLEB128;

import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Datatype.JSON;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode.RANGE_SCAN_CREATE;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.CbCollections.mapOf;

public class RangeScanCreateRequest extends PredeterminedPartitionRequest<RangeScanCreateResponse> {

  private final byte[] startTerm;
  private final boolean startExclusive;
  private final byte[] endTerm;
  private final boolean endExclusive;
  private final long limit;
  private final Optional<Long> seed;

  private final boolean keyOnly;

  private final Optional<MutationToken> mutationToken;

  public static RangeScanCreateRequest forRangeScan(byte[] startTerm, boolean startExclusive, byte[] endTerm,
                                                    boolean endExclusive, boolean keyOnly, Duration timeout, CoreContext ctx,
                                                    RetryStrategy retryStrategy,
                                                    CollectionIdentifier collectionIdentifier, RequestSpan span,
                                                    short partition, Optional<MutationToken> mutationToken) {
    return new RangeScanCreateRequest(startTerm, startExclusive, endTerm, endExclusive, 0, Optional.empty(),
      keyOnly, timeout, ctx, retryStrategy, collectionIdentifier, span, partition, mutationToken);
  }

  public static RangeScanCreateRequest forSamplingScan(long limit, Optional<Long> seed, boolean keyOnly,
                                                       Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                                       CollectionIdentifier collectionIdentifier, RequestSpan span,
                                                       short partition, Optional<MutationToken> mutationToken) {
    return new RangeScanCreateRequest(null, false, null, false, limit, seed,
      keyOnly, timeout, ctx, retryStrategy, collectionIdentifier, span, partition, mutationToken);
  }

  private RangeScanCreateRequest(byte[] startTerm, boolean startExclusive, byte[] endTerm,
                                 boolean endExclusive, long limit, Optional<Long> seed, boolean keyOnly,
                                 Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                 CollectionIdentifier collectionIdentifier, RequestSpan span, short partition,
                                 Optional<MutationToken> mutationToken) {
    super(partition, timeout, ctx, retryStrategy, null, collectionIdentifier, span);
    this.startTerm = startTerm;
    this.startExclusive = startExclusive;
    this.endTerm = endTerm;
    this.endExclusive = endExclusive;
    this.limit = limit;
    this.seed = seed;
    this.keyOnly = keyOnly;
    this.mutationToken = mutationToken;
  }

  private boolean isRangeScanFlavour() {
    return startTerm != null && endTerm != null;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    byte[] encodedCollectionId = ctx.collectionMap().get(collectionIdentifier());
    if (encodedCollectionId == null) {
      throw CollectionNotFoundException.forCollection(collectionIdentifier().collection().orElse(""));
    }

    long collectionId = UnsignedLEB128.decode(encodedCollectionId);
    Map<String, Object> payload = new HashMap<>();
    if (collectionId != 0) {
      payload.put("collection", Long.toHexString(collectionId));
    }
    if (keyOnly) {
      payload.put("key_only", true);
    }

    mutationToken.ifPresent(token -> payload.put("snapshot_requirements", mapOf(
      "vb_uuid", Long.toString(token.partitionUUID()),
      "seqno", token.sequenceNumber(),
      "timeout_ms", Math.toIntExact(timeout().toMillis())
    )));

    if (isRangeScanFlavour()) {
      payload.put("range", mapOf(
        startExclusive ? "excl_start" : "start", Base64.getEncoder().encodeToString(startTerm),
        endExclusive ? "excl_end" : "end", Base64.getEncoder().encodeToString(endTerm)
      ));
    } else {
      payload.put("sampling", mapOf(
        "samples", limit,
        "seed", seed
          // Server drops connection if negative. Force positive by clearing sign bit.
          .map(it -> it & 0x7fffffffffffffffL)
          .orElse(0L)
      ));
    }

    ByteBuf body = Unpooled.wrappedBuffer(Mapper.encodeAsBytes(payload));
    try {
      return MemcacheProtocol.request(alloc, RANGE_SCAN_CREATE, JSON.datatype(), partition(), opaque,
        noCas(), noExtras(), noKey(), body);
    } finally {
      body.release();
    }
  }

  @Override
  public RangeScanCreateResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    ByteBuf body = MemcacheProtocol.body(response).orElse(Unpooled.EMPTY_BUFFER);
    CoreRangeScanId scanId = status.success() ? new CoreRangeScanId(body) : null;
    return new RangeScanCreateResponse(status, scanId);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String toString() {
    return "RangeScanCreateRequest{" +
      "startTerm=" + redactUser(Arrays.toString(startTerm)) +
      ", startExclusive=" + startExclusive +
      ", endTerm=" + redactUser(Arrays.toString(endTerm)) +
      ", endExclusive=" + endExclusive +
      ", limit=" + limit +
      ", seed=" + seed +
      ", keyOnly=" + keyOnly +
      ", mutationToken=" + redactMeta(mutationToken) +
      '}';
  }
}
