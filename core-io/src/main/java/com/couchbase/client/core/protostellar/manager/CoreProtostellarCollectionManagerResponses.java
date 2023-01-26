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

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.config.CollectionsManifestCollection;
import com.couchbase.client.core.config.CollectionsManifestScope;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.protostellar.admin.collection.v1.CreateCollectionResponse;
import com.couchbase.client.protostellar.admin.collection.v1.CreateScopeResponse;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteCollectionResponse;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteScopeResponse;
import com.couchbase.client.protostellar.admin.collection.v1.ListCollectionsResponse;

/**
 * For converting Protostellar GRPC responses.
 */
@Stability.Internal
public class CoreProtostellarCollectionManagerResponses {
  private CoreProtostellarCollectionManagerResponses() {}

  public static CollectionsManifest convertResponse(ListCollectionsResponse response) {
    List<CollectionsManifestScope> cmScopes = new LinkedList();
    for (ListCollectionsResponse.Scope s : response.getScopesList()) {
      List<CollectionsManifestCollection> collections = new LinkedList();
      for (ListCollectionsResponse.Collection c : s.getCollectionsList()) {
        CollectionsManifestCollection collection = new CollectionsManifestCollection(c.getName(), c.getName(), 0);
        collections.add(collection);
      }
      CollectionsManifestScope scope = new CollectionsManifestScope(s.getName(), s.getName(), collections);
      cmScopes.add(scope);
    }
    return new CollectionsManifest(UUID.randomUUID().toString(), cmScopes);
  }
}
