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
package com.couchbase.client.core.protostellar.manager;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;

import java.time.Duration;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarCollectionManagerRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.ListCollectionsRequest;

/**
 * For creating Protostellar GRPC requests.
 */
@Stability.Internal
public class CoreProtostellarCollectionManagerRequests {
  private CoreProtostellarCollectionManagerRequests() {}

  public static ProtostellarRequest<CreateCollectionRequest> createCollectionRequest(Core core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     Duration maxTTL,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
    ProtostellarRequest<CreateCollectionRequest> out = new ProtostellarCollectionManagerRequest<>(core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    CreateCollectionRequest.Builder request = CreateCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName);
      //.setMaxTTL(maxTTL)

    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<DeleteCollectionRequest> deleteCollectionRequest(Core core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
    ProtostellarRequest<DeleteCollectionRequest> out = new ProtostellarCollectionManagerRequest<>(core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    DeleteCollectionRequest.Builder request = DeleteCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName);

    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<CreateScopeRequest> createScopeRequest(Core core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
    ProtostellarRequest<CreateScopeRequest> out = new ProtostellarCollectionManagerRequest<>(core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    CreateScopeRequest.Builder request = CreateScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName);
    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<DeleteScopeRequest> deleteScopeRequest(Core core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
    ProtostellarRequest<DeleteScopeRequest> out = new ProtostellarCollectionManagerRequest<>(core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    DeleteScopeRequest.Builder request = DeleteScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName);
    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<ListCollectionsRequest> listCollectionsRequest(Core core,
                                                                                   String bucketName,
                                                                                   CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);
    ProtostellarRequest<ListCollectionsRequest> out = new ProtostellarCollectionManagerRequest<>(core,
      bucketName, null, null,
      TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      true,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());

    ListCollectionsRequest.Builder request = ListCollectionsRequest.newBuilder()
      .setBucketName(bucketName);
    out.request(request.build());
    return out;
  }
}
