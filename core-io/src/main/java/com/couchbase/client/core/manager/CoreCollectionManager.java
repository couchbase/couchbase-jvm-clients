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
package com.couchbase.client.core.manager;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.manager.collection.CoreCreateOrUpdateCollectionSettings;

import java.util.concurrent.CompletableFuture;

@Stability.Internal
public interface CoreCollectionManager {
  CompletableFuture<Void> createCollection(String scopeName,
                                           String collectionName,
                                           CoreCreateOrUpdateCollectionSettings settings,
                                           CoreCommonOptions options);

  CompletableFuture<Void> updateCollection(String scopeName,
                                           String collectionName,
                                           CoreCreateOrUpdateCollectionSettings settings,
                                           CoreCommonOptions options);

  CompletableFuture<Void> createScope(String scopeName, CoreCommonOptions options);

  CompletableFuture<Void> dropCollection(String scopeName, String collectionName, CoreCommonOptions options);

  CompletableFuture<Void> dropScope(String scopeName, CoreCommonOptions options);

  CompletableFuture<CollectionsManifest> getAllScopes(CoreCommonOptions options);
}
