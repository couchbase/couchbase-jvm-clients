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

import static com.couchbase.client.core.protostellar.kv.CoreProtoStellarKvBinaryRequests.appendRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtoStellarKvBinaryRequests.decrementRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtoStellarKvBinaryRequests.incrementRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtoStellarKvBinaryRequests.prependRequest;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvBinaryParamValidators;
import reactor.core.publisher.Mono;

import java.util.Optional;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreCounterResult;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;

@Stability.Internal
public class ProtostellarCoreKvBinaryOps implements CoreKvBinaryOps {

  private final Core core;
  private final CoreKeyspace keyspace;

  public ProtostellarCoreKvBinaryOps(Core core, CoreKeyspace keyspace) {
    CoreKvBinaryParamValidators.validateCore(core);
    CoreKvBinaryParamValidators.validateKeyspace(keyspace);
    this.core = core;
    this.keyspace = keyspace;
  }

  @Override
  public CoreMutationResult appendBlocking(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.AppendRequest> request = appendRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.blocking(core, request,
        (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).append(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> appendAsync(String id, byte[] content, CoreCommonOptions options,
      long cas, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.AppendRequest> request = appendRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).append(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public Mono<CoreMutationResult> appendReactive(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.AppendRequest> request = appendRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.reactive(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).append(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreMutationResult prependBlocking(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.PrependRequest> request = prependRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.blocking(core, request,
        (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).prepend(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> prependAsync(String id, byte[] content, CoreCommonOptions options,
      long cas, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.PrependRequest> request = prependRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).prepend(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public Mono<CoreMutationResult> prependReactive(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.PrependRequest> request = prependRequest(core, id,
        keyspace, options, content, cas, durability);
    return CoreProtostellarAccessors.reactive(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).prepend(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreCounterResult incrementBlocking(String id, CoreCommonOptions options, long expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.IncrementRequest> request = incrementRequest(core, id,
        keyspace, options, expiry, delta, initial, durability);
    return CoreProtostellarAccessors.blocking(core, request,
        (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).increment(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreAsyncResponse<CoreCounterResult> incrementAsync(String id, CoreCommonOptions options, long expiry,
      long delta, Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.IncrementRequest> request = incrementRequest(core, id,
        keyspace, options, expiry, delta, initial,  durability);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).increment(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public Mono<CoreCounterResult> incrementReactive(String id, CoreCommonOptions options, long expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.IncrementRequest> request = incrementRequest(core, id,
        keyspace, options, expiry, delta, initial , durability);
    return CoreProtostellarAccessors.reactive(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).increment(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreCounterResult decrementBlocking(String id, CoreCommonOptions options, long expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.DecrementRequest> request = decrementRequest(core, id,
        keyspace, options, expiry, delta, initial, durability);
    return CoreProtostellarAccessors.blocking(core, request,
        (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).decrement(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public CoreAsyncResponse<CoreCounterResult> decrementAsync(String id, CoreCommonOptions options, long expiry,
      long delta, Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.DecrementRequest> request = decrementRequest(core, id,
        keyspace, options, expiry, delta, initial, durability);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).decrement(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

  @Override
  public Mono<CoreCounterResult> decrementReactive(String id, CoreCommonOptions options, long expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.DecrementRequest> request = decrementRequest(core, id,
        keyspace, options, expiry, delta, initial,  durability);
    return CoreProtostellarAccessors.reactive(core, request,
        (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).decrement(request.request()),
        (response) -> CoreProtostellarKvBinaryResponses.convertResponse(keyspace, id, response));
  }

}
