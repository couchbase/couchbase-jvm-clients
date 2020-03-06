/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.msg.UnmonitoredRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.datatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.tryDecompression;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * A request to fetch a global configuration.
 *
 * <p>Note that this request is technicall the same as a {@link CarrierBucketConfigRequest}, but it makes it clear
 * that it is not tied to a bucket (and as a result does not accept one when being constructed).</p>
 */
public class CarrierGlobalConfigRequest
  extends BaseKeyValueRequest<CarrierGlobalConfigResponse>
  implements TargetedRequest, UnmonitoredRequest {

  private final NodeIdentifier target;

  public CarrierGlobalConfigRequest(final Duration timeout, final CoreContext ctx, final RetryStrategy retryStrategy,
                                    final NodeIdentifier target) {
    super(timeout, ctx, retryStrategy, null, null);
    this.target = target;
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque, final KeyValueChannelContext ctx) {
    return MemcacheProtocol.request(alloc, Opcode.GET_CONFIG, noDatatype(),
      noPartition(), opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public CarrierGlobalConfigResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    byte[] content = body(response)
      .map(ByteBufUtil::getBytes)
      .map(bytes -> tryDecompression(bytes, datatype(response)))
      .orElse(Bytes.EMPTY_BYTE_ARRAY);
    return new CarrierGlobalConfigResponse(decodeStatus(response), content);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

  @Override
  public Map<String, Object> serviceContext() {
    final Map<String, Object> ctx = super.serviceContext();
    if (target != null) {
      ctx.put("target", redactSystem(target.address()));
    }
    return ctx;
  }

}
