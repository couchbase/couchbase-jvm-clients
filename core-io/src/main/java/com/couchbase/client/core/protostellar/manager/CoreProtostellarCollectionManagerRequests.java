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

import com.couchbase.client.core.CoreProtostellar;
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

import java.time.Duration;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;

/**
 * For creating Protostellar GRPC requests.
 */
@Stability.Internal
public class CoreProtostellarCollectionManagerRequests {
  private CoreProtostellarCollectionManagerRequests() {
  }

  public static ProtostellarRequest<CreateCollectionRequest> createCollectionRequest(CoreProtostellar core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     Duration maxTTL,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    CreateCollectionRequest request = CreateCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName)
      // This functionality is missing from Protostellar currently.
      //.setMaxTTL(maxTTL)
      .build();

    return new ProtostellarCollectionManagerRequest<>(request,
      core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());
  }

  public static ProtostellarRequest<DeleteCollectionRequest> deleteCollectionRequest(CoreProtostellar core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    DeleteCollectionRequest request = DeleteCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName)
      .build();

    return new ProtostellarCollectionManagerRequest<>(request,
      core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());
  }

  public static ProtostellarRequest<CreateScopeRequest> createScopeRequest(CoreProtostellar core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    CreateScopeRequest request = CreateScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .build();

    return new ProtostellarCollectionManagerRequest<>(request,
      core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());
  }

  public static ProtostellarRequest<DeleteScopeRequest> deleteScopeRequest(CoreProtostellar core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    DeleteScopeRequest request = DeleteScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .build();

    return new ProtostellarCollectionManagerRequest<>(request,
      core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());
  }

  public static ProtostellarRequest<ListCollectionsRequest> listCollectionsRequest(CoreProtostellar core,
                                                                                   String bucketName,
                                                                                   CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    ListCollectionsRequest request = ListCollectionsRequest.newBuilder()
      .setBucketName(bucketName)
      .build();

    return new ProtostellarCollectionManagerRequest<>(request,
      core,
      bucketName, null, null,
      TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      true,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext());
  }
}
