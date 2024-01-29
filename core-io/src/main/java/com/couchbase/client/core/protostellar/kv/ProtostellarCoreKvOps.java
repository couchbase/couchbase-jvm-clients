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

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.api.kv.CoreExistsResult;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.api.kv.CoreStoreSemantics;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.api.kv.CoreSubdocMutateCommand;
import com.couchbase.client.core.api.kv.CoreSubdocMutateResult;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.CoreScanOptions;
import com.couchbase.client.core.kv.CoreScanType;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.existsRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.getAndLockRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.getAndTouchRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.getRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.insertRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.lookupInRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.mutateInRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.removeRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.replaceRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.touchRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.unlockRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueRequests.upsertRequest;
import static com.couchbase.client.core.protostellar.kv.CoreProtostellarKeyValueResponses.convertResponse;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ProtostellarCoreKvOps implements CoreKvOps {
  private final CoreProtostellar core;
  private final CoreKeyspace keyspace;

  public ProtostellarCoreKvOps(CoreProtostellar core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.keyspace = requireNonNull(keyspace);
  }

  @Override
  public CoreGetResult getBlocking(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key, projections, withExpiry);

    return CoreProtostellarAccessors.blocking(core,
      req,
      (endpoint) -> {
        // withDeadline creates a new stub and Google performance docs advise reusing stubs as much as possible.
        // However, we've measured the impact and found zero difference.
        return endpoint.kvBlockingStub().withDeadline(req.deadline()).get(req.request());
      },
      (response) -> convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAsync(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key, projections, withExpiry);

    return CoreProtostellarAccessors.async(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).get(req.request()),
      (response) -> convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreGetResult> getReactive(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = getRequest(core, common, keyspace, key, projections, withExpiry);

    return CoreProtostellarAccessors.reactive(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).get(req.request()),
      (response) -> convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndLockAsync(CoreCommonOptions common, String key, Duration lockTime) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetAndLockRequest> req = getAndLockRequest(core, common, keyspace, key, lockTime);
    return CoreProtostellarAccessors.async(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).getAndLock(req.request()),
      (response) -> convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndTouchAsync(CoreCommonOptions common, String key, CoreExpiry expiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetAndTouchRequest> req = getAndTouchRequest(core, common, keyspace, key, expiry);
    return CoreProtostellarAccessors.async(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(req.deadline()).getAndTouch(req.request()),
      (response) -> convertResponse(keyspace, key, response));
  }

  @Override
  public CoreMutationResult insertBlocking(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> insertAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> insertReactive(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.InsertRequest> request = insertRequest(core, keyspace, common, key, content, durability, expiry);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).insert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreMutationResult upsertBlocking(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.UpsertRequest> request = upsertRequest(core, keyspace, common, key, content, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).upsert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> upsertAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.UpsertRequest> request = upsertRequest(core, keyspace, common, key, content, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).upsert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> upsertReactive(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.UpsertRequest> request = upsertRequest(core, keyspace, common, key, content, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).upsert(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreMutationResult replaceBlocking(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ReplaceRequest> request = replaceRequest(core, keyspace, common, key, content, cas, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).replace(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> replaceAsync(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ReplaceRequest> request = replaceRequest(core, keyspace, common, key, content, cas, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).replace(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> replaceReactive(CoreCommonOptions common, String key, Supplier<CoreEncodedContent> content, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ReplaceRequest> request = replaceRequest(core, keyspace, common, key, content, cas, durability, expiry, preserveExpiry);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).replace(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreMutationResult removeBlocking(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> removeAsync(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreMutationResult> removeReactive(CoreCommonOptions common, String key, long cas, CoreDurability durability) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request = removeRequest(core, keyspace, common, key, cas, durability);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).remove(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreExistsResult existsBlocking(CoreCommonOptions common, String key) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ExistsRequest> request = existsRequest(core, keyspace, common, key);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).exists(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreExistsResult> existsAsync(CoreCommonOptions common, String key) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ExistsRequest> request = existsRequest(core, keyspace, common, key);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).exists(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public Mono<CoreExistsResult> existsReactive(CoreCommonOptions common, String key) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.ExistsRequest> request = existsRequest(core, keyspace, common, key);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).exists(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<CoreMutationResult> touchAsync(CoreCommonOptions common, String key, CoreExpiry expiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.TouchRequest> request = touchRequest(core, keyspace, common, key, expiry);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).touch(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response));
  }

  @Override
  public CoreAsyncResponse<Void> unlockAsync(CoreCommonOptions common, String key, long cas) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.UnlockRequest> request = unlockRequest(core, keyspace, common, key, cas);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).unlock(request.request()),
      (response) -> null);
  }

  @Override
  public CoreSubdocGetResult subdocGetBlocking(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands, boolean accessDeleted) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.LookupInRequest> request = lookupInRequest(core, keyspace, common, key, commands, accessDeleted);
    return CoreProtostellarAccessors.blocking(core,
            request,
            (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).lookupIn(request.request()),
            (response) -> CoreProtostellarKeyValueResponses.convertResponse(core, request, keyspace, key, response, commands));
  }

  @Override
  public CoreAsyncResponse<CoreSubdocGetResult> subdocGetAsync(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands, boolean accessDeleted) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.LookupInRequest> request = lookupInRequest(core, keyspace, common, key, commands, accessDeleted);
    return CoreProtostellarAccessors.async(core,
            request,
            (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).lookupIn(request.request()),
            (response) -> CoreProtostellarKeyValueResponses.convertResponse(core, request, keyspace, key, response, commands));
  }

  @Override
  public Mono<CoreSubdocGetResult> subdocGetReactive(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands, boolean accessDeleted) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.LookupInRequest> request = lookupInRequest(core, keyspace, common, key, commands, accessDeleted);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).lookupIn(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(core, request, keyspace, key, response, commands));
  }


  @Override
  public Flux<CoreSubdocGetResult> subdocGetAllReplicasReactive(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands) {
    // Protostellar subdoc-from-replica support is currently incomplete.
    throw unsupported();
  }

  @Override
  public Mono<CoreSubdocGetResult> subdocGetAnyReplicaReactive(CoreCommonOptions common, String key, List<CoreSubdocGetCommand> commands) {
    // Protostellar subdoc-from-replica support is currently incomplete.
    throw unsupported();
  }

  @Override
  public Flux<CoreGetResult> getAllReplicasReactive(CoreCommonOptions common, String key) {
    // Protostellar get-from-replica support is currently incomplete.  JVMCBC-1263.
    throw unsupported();
  }

  @Override
  public Mono<CoreGetResult> getAnyReplicaReactive(CoreCommonOptions common, String key) {
    // Protostellar get-from-replica support is currently incomplete.  JVMCBC-1263.
    throw unsupported();
  }

  @Override
  public CoreSubdocMutateResult subdocMutateBlocking(CoreCommonOptions common, String key, Supplier<List<CoreSubdocMutateCommand>> commands, CoreStoreSemantics storeSemantics, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry, boolean accessDeleted, boolean createAsDeleted) {
    List<CoreSubdocMutateCommand> specs = commands.get();
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.MutateInRequest> request = mutateInRequest(core, keyspace, common, key, specs, storeSemantics, cas, durability, expiry, preserveExpiry, accessDeleted, createAsDeleted);
    return CoreProtostellarAccessors.blocking(core,
      request,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(request.deadline()).mutateIn(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response, specs));
  }

  @Override
  public CoreAsyncResponse<CoreSubdocMutateResult> subdocMutateAsync(CoreCommonOptions common, String key, Supplier<List<CoreSubdocMutateCommand>> commands, CoreStoreSemantics storeSemantics, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry, boolean accessDeleted, boolean createAsDeleted) {
    List<CoreSubdocMutateCommand> specs = commands.get();
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.MutateInRequest> request = mutateInRequest(core, keyspace, common, key, specs, storeSemantics, cas, durability, expiry, preserveExpiry, accessDeleted, createAsDeleted);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).mutateIn(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response, specs));
  }

  @Override
  public Mono<CoreSubdocMutateResult> subdocMutateReactive(CoreCommonOptions common, String key, Supplier<List<CoreSubdocMutateCommand>> commands, CoreStoreSemantics storeSemantics, long cas, CoreDurability durability, CoreExpiry expiry, boolean preserveExpiry, boolean accessDeleted, boolean createAsDeleted) {
    List<CoreSubdocMutateCommand> specs = commands.get();
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.MutateInRequest> request = mutateInRequest(core, keyspace, common, key, specs, storeSemantics, cas, durability, expiry, preserveExpiry, accessDeleted, createAsDeleted);
    return CoreProtostellarAccessors.reactive(core,
      request,
      (endpoint) -> endpoint.kvStub().withDeadline(request.deadline()).mutateIn(request.request()),
      (response) -> CoreProtostellarKeyValueResponses.convertResponse(keyspace, key, response, specs));
  }

  @Override
  public Flux<CoreRangeScanItem> scanRequestReactive(CoreScanType scanType, CoreScanOptions options) {
    throw unsupported();
  }

  @Override
  public CompletableFuture<List<CoreRangeScanItem>> scanRequestAsync(CoreScanType scanType, CoreScanOptions options) {
    throw unsupported();
  }

  @Override
  public Stream<CoreRangeScanItem> scanRequestBlocking(CoreScanType scanType, CoreScanOptions options) {
    throw unsupported();
  }

  private static RuntimeException unsupported() {
    return new FeatureNotAvailableException("Not currently supported in couchbase2");
  }
}
