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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.kv.KeyValuePacket;
import com.couchbase.client.core.io.kv.KeyValueRequestEncoder;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Opcode;
import com.couchbase.client.core.io.netty.kv.sasl.SingleStepSaslAuthParameters;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.FailFastRetryStrategy;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Reauthenticates a KV connection with a single SASL_AUTH request, when the authenticator's credentials change.
 */
@Stability.Internal
public class SingleStepAuthRequest extends BaseKeyValueRequest<SingleStepAuthResponse> {
  private final SingleStepSaslAuthParameters params;

  public SingleStepAuthRequest(CoreContext ctx, SingleStepSaslAuthParameters params) {
    // We're sending this request at a low layer that bypasses timeout and retry handling,
    // so those arguments don't really matter -- but superclass requires us to pass _something_.
    super(Duration.ZERO, ctx, FailFastRetryStrategy.INSTANCE, null, null, null);

    this.params = requireNonNull(params);
  }

  @Override
  public void encodeTo(KeyValueRequestEncoder encoder) {
    encoder.beginRequest(Opcode.SASL_AUTH)
      .key(params.mechanism().mech().getBytes(UTF_8))
      .value(params.payload().getBytes(UTF_8));
  }

  @Override
  public SingleStepAuthResponse decode(KeyValuePacket response, KeyValueChannelContext ctx) {
    short statusCode = response.status();
    ResponseStatus status = statusCode == MemcacheProtocol.Status.AUTH_ERROR.status()
      ? ResponseStatus.NO_ACCESS // abusing this to mean "not authenticated"
      : decodeStatus(statusCode);

    return new SingleStepAuthResponse(status);
  }
}
