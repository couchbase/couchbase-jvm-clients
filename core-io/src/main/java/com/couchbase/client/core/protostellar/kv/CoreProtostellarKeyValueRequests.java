/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.kv.v1.InsertRequest;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertFromFlags;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_GET;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_INSERT;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_REMOVE;

/**
 * For creating Protostellar GRPC KV requests.
 */
@Stability.Internal
public class CoreProtostellarKeyValueRequests {
  private CoreProtostellarKeyValueRequests() {}

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> getRequest(Core core,
                                                                                                   CoreCommonOptions opts,
                                                                                                   CoreKeyspace keyspace,
                                                                                                   String key) {
    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> out = new ProtostellarKeyValueRequest<>(core,
      keyspace,
      key,
      CoreDurability.NONE,
      REQUEST_KV_GET,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      true,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    out.request(com.couchbase.client.protostellar.kv.v1.GetRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .build());

    return out;
  }

  public static ProtostellarRequest<InsertRequest> insertRequest(Core core,
                                                                 CoreKeyspace keyspace,
                                                                 CoreCommonOptions opts,
                                                                 String key,
                                                                 Supplier<CoreEncodedContent> content,
                                                                 CoreDurability durability,
                                                                 long expiry) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);
    ProtostellarRequest<InsertRequest> out = new ProtostellarKeyValueRequest<>(core,
      keyspace,
      key,
      CoreDurability.NONE,
      REQUEST_KV_INSERT,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_INSERT, durability, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    final RequestSpan encodeSpan = CbTracing.newSpan(core.context(), TracingIdentifiers.SPAN_REQUEST_ENCODING, out.span());
    long start = System.nanoTime();
    CoreEncodedContent encoded;
    try {
      encoded = content.get();
    } finally {
      encodeSpan.end();
    }

    out.encodeLatency(System.nanoTime() - start);

    InsertRequest.Builder request = InsertRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setContent(ByteString.copyFrom(encoded.encoded()))
      .setContentType(convertFromFlags(encoded.flags()));

    if (expiry != 0) {
      request.setExpiry(CoreProtostellarUtil.convertExpiry(expiry));
    }
    if (!durability.isNone()) {
      request.setDurabilityLevel(CoreProtostellarUtil.convert(durability));
    }

    out.request(request.build());

    return out;
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> removeRequest(Core core,
                                                                                                         CoreKeyspace keyspace,
                                                                                                         CoreCommonOptions opts,
                                                                                                         String key,
                                                                                                         long cas,
                                                                                                         CoreDurability durability) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), durability, core);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> out = new ProtostellarKeyValueRequest<>(core,
      keyspace,
      key,
      CoreDurability.NONE,
      REQUEST_KV_REMOVE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE, durability, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    com.couchbase.client.protostellar.kv.v1.RemoveRequest.Builder request = com.couchbase.client.protostellar.kv.v1.RemoveRequest.newBuilder()
      .setBucketName(keyspace.bucket())
      .setScopeName(keyspace.scope())
      .setCollectionName(keyspace.collection())
      .setKey(key)
      .setCas(cas);

    if (!durability.isNone()) {
      request.setDurabilityLevel(CoreProtostellarUtil.convert(durability));
    }

    out.request(request.build());
    return out;
  }

}
