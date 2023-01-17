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
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateGetParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateInsertParams;
import static com.couchbase.client.core.api.kv.CoreKvParamValidators.validateRemoveParams;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.getRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.insertRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.removeRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueResponses.convertGetResponse;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ProtostellarCoreKvOps implements CoreKvOps {
  private final Core core;
  private final CoreKeyspace keyspace;

  public ProtostellarCoreKvOps(Core core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.keyspace = requireNonNull(keyspace);
  }

  @Override
  public CoreGetResult getBlocking(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    validateGetParams(common, key, projections, withExpiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key);

    return CoreProtostellarAccessors.blocking(core,
      req,
      (endpoint) -> {
        // withDeadline creates a new stub and Google performance docs advise reusing stubs as much as possible.
        // However, we've measured the impact and found zero difference.
        return endpoint.kvBlockingStub().withDeadline(req.deadline()).get(req.request());
      },
      (response) -> convertGetResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAsync(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    validateGetParams(common, key, projections, withExpiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key);

    return CoreProtostellarAccessors.async(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).get(req.request()),
      (response) -> convertGetResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreGetResult> getReactive(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    validateGetParams(common, key, projections, withExpiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key);

    return CoreProtostellarAccessors.reactive(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).get(req.request()),
      (response) -> convertGetResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndLockAsync(
    CoreCommonOptions common,
    String key,
    Duration lockTime
  ) {
    throw unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
    CoreCommonOptions common,
    String key,
    long expiration
  ) {
    throw unsupported();
  }

  @Override
  public CoreMutationResult insertBlocking(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, long expiry) {
    validateInsertParams(common, key, content, durability, expiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> insertAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, long expiry) {
    validateInsertParams(common, key, content, durability, expiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> insertReactive(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, long expiry) {
    validateInsertParams(common, key, content, durability, expiry);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> upsertAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, long expiry, boolean preserveExpiry) {
    throw unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> replaceAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, long cas, CoreDurability durability, long expiry, boolean preserveExpiry) {
    throw unsupported();
  }

  @Override
  public CoreMutationResult removeBlocking(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    validateRemoveParams(common, key, cas, durability);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> removeAsync(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    validateRemoveParams(common, key, cas, durability);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> removeReactive(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    validateRemoveParams(common, key, cas, durability);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreExistsResult> existsAsync(CoreCommonOptions common, String key) {
    throw unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> touchAsync(CoreCommonOptions common, String key, long expiry) {
    throw unsupported();
  }

  @Override
  public CoreAsyncResponse<Void> unlockAsync(CoreCommonOptions common, String key, long cas) {
    throw unsupported();
  }

  @Override
  public Flux<CoreGetResult> getAllReplicasReactive(CoreCommonOptions common, String key) {
    throw unsupported();
  }

  @Override
  public Mono<CoreGetResult> getAnyReplicaReactive(CoreCommonOptions common, String key) {
    throw unsupported();
  }

  private static RuntimeException unsupported() {
    return new UnsupportedOperationException("Not currently supported");
  }
}
