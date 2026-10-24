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
import com.couchbase.client.core.io.KeyValuePacket;
import com.couchbase.client.core.io.KeyValueRequestBuilder;
import com.couchbase.client.core.io.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.msg.UnmonitoredRequest;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * A request to fetch a global configuration.
 *
 * <p>Note that this request is technicall the same as a {@link CarrierBucketConfigRequest}, but it makes it clear
 * that it is not tied to a bucket (and as a result does not accept one when being constructed).</p>
 */
public class CarrierGlobalConfigRequest
  extends BasePortableKeyValueRequest<CarrierGlobalConfigResponse>
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
    super(timeout, ctx, retryStrategy, null, null, null);
    this.target = target;
    this.ifNewerThan = Optional.ofNullable(ifNewerThan).orElse(ConfigVersion.ZERO);
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    encodeConfigRequest(encoder, ifNewerThan);
  }

  static void encodeConfigRequest(
    KeyValueRequestEncoder encoder,
    ConfigVersion ifNewerThan
  ) {
    KeyValueRequestBuilder request = encoder.beginRequest(Opcode.GET_CONFIG);
    if (encoder.context().getClusterConfigWithKnownVersion() && !ifNewerThan.isLessThanOrEqualTo(ConfigVersion.ZERO)) {
      request.extras()
        .putLong(ifNewerThan.epoch())
        .putLong(ifNewerThan.rev());
    }
  }

  @Override
  public CarrierGlobalConfigResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    return new CarrierGlobalConfigResponse(response.responseStatus(), response.bodyAsBytes());
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
