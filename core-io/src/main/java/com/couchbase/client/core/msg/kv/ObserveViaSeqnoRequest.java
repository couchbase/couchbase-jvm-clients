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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class ObserveViaSeqnoRequest extends BaseKeyValueRequest<ObserveViaSeqnoResponse> {

  private final int replica;
  private final boolean active;
  private final long vbucketUUID;

  public ObserveViaSeqnoRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                                final RetryStrategy retryStrategy,
                                final int replica, final boolean active, final long vbucketUUID, final String key, final RequestSpan span) {
    super(timeout, ctx, retryStrategy, key, collectionIdentifier, span);
    this.replica = replica;
    this.active = active;
    this.vbucketUUID = vbucketUUID;

    if (span != null) {
      span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TracingIdentifiers.SPAN_REQUEST_KV_OBSERVE);
    }
  }

  @Override
  public int replica() {
    return replica;
  }

  public boolean active() {
    return active;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    ByteBuf body = null;
    try {
      body = alloc.buffer(Long.BYTES).writeLong(vbucketUUID);
      return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.OBSERVE_SEQ, noDatatype(), partition(),
        opaque, noCas(), noExtras(), noKey(), body);
    } finally {
      ReferenceCountUtil.release(body);
    }
  }

  @Override
  public ObserveViaSeqnoResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    ResponseStatus status = decodeStatus(response);
    if (status.success()) {
      ByteBuf content = body(response).get();
      byte format = content.readByte();
      short vbucketId = content.readShort();
      long vbucketUUID = content.readLong();
      long lastPersistedSeqno = content.readLong();
      long currentSeqno = content.readLong();
      switch (format) {
        case 0:
          return new ObserveViaSeqnoResponse(status, active, vbucketId, vbucketUUID, lastPersistedSeqno, currentSeqno,
            Optional.empty(), Optional.empty());
        case 1:
          return new ObserveViaSeqnoResponse(status, active, vbucketId,vbucketUUID, lastPersistedSeqno, currentSeqno,
            Optional.of(content.readLong()), Optional.of(content.readLong()));
        default:
          throw new IllegalStateException("Unsupported format 0x" + Integer.toHexString(format));
      }
    } else {
      return new ObserveViaSeqnoResponse(status, active, (short) 0, 0L, 0L, 0L,
        Optional.empty(), Optional.empty());
    }
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public String name() {
    return "observe";
  }

}
