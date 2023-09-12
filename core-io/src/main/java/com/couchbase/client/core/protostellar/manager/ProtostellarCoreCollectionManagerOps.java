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
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.manager.CoreCollectionManager;
import com.couchbase.client.core.manager.collection.CoreCreateOrUpdateCollectionSettings;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.ListCollectionsRequest;
import reactor.util.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedCurrentlyInProtostellar;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerRequests.createCollectionRequest;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerRequests.createScopeRequest;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerRequests.deleteCollectionRequest;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerRequests.deleteScopeRequest;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerRequests.listCollectionsRequest;
import static com.couchbase.client.core.protostellar.manager.CoreProtostellarCollectionManagerResponses.convertResponse;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ProtostellarCoreCollectionManagerOps implements CoreCollectionManager {
  private final CoreProtostellar core;
  private final String bucketName;

  public ProtostellarCoreCollectionManagerOps(CoreProtostellar core, String bucketName) {
    this.core = requireNonNull(core);
    this.bucketName = requireNonNull(bucketName);
  }

  @Override
  public CompletableFuture<Void> createCollection(String scopeName,
                                                  String collectionName,
                                                  @Nullable CoreCreateOrUpdateCollectionSettings settings,
                                                  CoreCommonOptions options) {
    ProtostellarRequest<CreateCollectionRequest> request = createCollectionRequest(core, bucketName, scopeName, collectionName, settings, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).createCollection(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<Void> updateCollection(String scopeName,
                                                  String collectionName,
                                                  @Nullable CoreCreateOrUpdateCollectionSettings settings,
                                                  CoreCommonOptions options) {
    throw unsupportedCurrentlyInProtostellar();
  }

  @Override
  public CompletableFuture<Void> createScope(String scopeName, CoreCommonOptions options) {
    ProtostellarRequest<CreateScopeRequest> request = createScopeRequest(core, bucketName, scopeName, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).createScope(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<Void> dropCollection(String scopeName, String collectionName, CoreCommonOptions options) {
    ProtostellarRequest<DeleteCollectionRequest> request = deleteCollectionRequest(core, bucketName, scopeName, collectionName, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).deleteCollection(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<Void> dropScope(String scopeName, CoreCommonOptions options) {
    ProtostellarRequest<DeleteScopeRequest> request = deleteScopeRequest(core, bucketName, scopeName, options);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).deleteScope(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<CollectionsManifest> getAllScopes(CoreCommonOptions options) {
    ProtostellarRequest<ListCollectionsRequest> request = listCollectionsRequest(core, bucketName, options);
    return CoreProtostellarAccessors.async(core, request,
        (endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).listCollections(request.request()),
        (response) -> convertResponse(response))
      .toFuture();
  }
}
