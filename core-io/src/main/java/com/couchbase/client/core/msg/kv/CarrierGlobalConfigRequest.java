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
import com.couchbase.client.core.config.ConfigVersion;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.msg.UnmonitoredRequest;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.bodyAsBytes;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
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
  private final ConfigVersion ifNewerThan;

  public CarrierGlobalConfigRequest(
    final Duration timeout,
    final CoreContext ctx,
    final RetryStrategy retryStrategy,
    final NodeIdentifier target,
    @Nullable final ConfigVersion ifNewerThan
  ) {
    super(timeout, ctx, retryStrategy, null, null);
    this.target = target;
    this.ifNewerThan = Optional.ofNullable(ifNewerThan).orElse(ConfigVersion.ZERO);
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public ByteBuf encode(
    final ByteBufAllocator alloc,
    final int opaque,
    final KeyValueChannelContext ctx
  ) {
    return encodeConfigRequest(alloc, opaque, ctx, ifNewerThan);
  }

  static ByteBuf encodeConfigRequest(
    final ByteBufAllocator alloc,
    final int opaque,
    final KeyValueChannelContext ctx,
    final ConfigVersion ifNewerThan
  ) {
    return MemcacheProtocol.request(
      alloc,
      Opcode.GET_CONFIG,
      noDatatype(),
      noPartition(),
      opaque,
      noCas(),
      requestExtras(ifNewerThan, ctx),
      noKey(),
      noBody()
    );
  }

  private static ByteBuf requestExtras(
    final ConfigVersion ifNewerThan,
    final KeyValueChannelContext ctx
  ) {
    if (!ctx.getClusterConfigWithKnownVersion() || ifNewerThan.isLessThanOrEqualTo(ConfigVersion.ZERO)) {
      return noExtras();
    }

    return Unpooled.buffer(16)
      .writeLong(ifNewerThan.epoch())
      .writeLong(ifNewerThan.rev());
  }

  @Override
  public CarrierGlobalConfigResponse decode(final ByteBuf response, final KeyValueChannelContext ctx) {
    byte[] content = bodyAsBytes(response);
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
      ctx.put("target", redactSystem(target));
    }
    return ctx;
  }

  @Override
  public String name() {
    return "carrier_global_config";
  }

  @Override
  public String toString() {
    return "CarrierGlobalConfigRequest{" +
      "target=" + redactSystem(target) +
      ", ifNewerThan=" + ifNewerThan +
      '}';
  }
}
