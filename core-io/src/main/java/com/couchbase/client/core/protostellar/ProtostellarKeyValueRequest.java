/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Internal
public class ProtostellarKeyValueRequest<TGrpcRequest> extends ProtostellarRequest<TGrpcRequest> {
  public ProtostellarKeyValueRequest(TGrpcRequest request,
                                     CoreProtostellar core,
                                     CoreKeyspace keyspace,
                                     String key,
                                     CoreDurability durability,
                                     String requestName,
                                     RequestSpan span,
                                     Duration timeout,
                                     boolean readonly,
                                     RetryStrategy retryStrategy,
                                     Map<String, Object> clientContext,
                                     long encodeDurationNanos) {
    super(request, core, ServiceType.KV, requestName, span, timeout, readonly, retryStrategy, clientContext, encodeDurationNanos, (ctx) -> {
      Map<String, Object> service = new HashMap<>();
      service.put("type", ServiceType.KV.ident());
      ctx.put("service", service);

      if (keyspace != null) {
        ctx.put("bucket", redactMeta(keyspace.bucket()));
        ctx.put("scope", redactMeta(keyspace.scope()));
        ctx.put("collection", redactMeta(keyspace.collection()));
      }

      if (key != null) {
        ctx.put("documentId", redactUser(key));
      }

      if (!durability.isLegacy() && !durability.isNone()) {
        ctx.put("syncDurability", durability.levelIfSynchronous().orElse(DurabilityLevel.NONE).encodeForManagementApi());
      }
    });
  }
}
