/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.collection;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.manager.CoreCollectionManager;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions;
import static com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions;
import static com.couchbase.client.java.manager.collection.DropCollectionOptions.dropCollectionOptions;
import static com.couchbase.client.java.manager.collection.DropScopeOptions.dropScopeOptions;
import static com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions;
import static com.couchbase.client.java.manager.collection.GetScopeOptions.getScopeOptions;

/**
 * The {@link AsyncCollectionManager} provides APIs to manage bucket collections and scopes.
 */
@Stability.Volatile
public class AsyncCollectionManager {
  private final CoreCollectionManager coreCollectionManager;

  public AsyncCollectionManager(Core core, String bucketName) {
    this.coreCollectionManager = new CoreCollectionManager(core, bucketName);
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection creation completed.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException    (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> createCollection(CollectionSpec collectionSpec) {
    return createCollection(collectionSpec, createCollectionOptions());
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection creation completed.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException    (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> createCollection(CollectionSpec collectionSpec,
                                                  CreateCollectionOptions options) {
    return coreCollectionManager.createCollection(
        collectionSpec.scopeName(),
        collectionSpec.name(),
        collectionSpec.maxExpiry(),
        options.build());
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link CompletableFuture} once the scope creation completed.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public CompletableFuture<Void> createScope(String scopeName) {
    return createScope(scopeName, createScopeOptions());
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link CompletableFuture} once the scope creation completed.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public CompletableFuture<Void> createScope(String scopeName, CreateScopeOptions options) {
    return coreCollectionManager.createScope(scopeName, options.build());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection is dropped.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException      (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> dropCollection(CollectionSpec collectionSpec) {
    return dropCollection(collectionSpec, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection is dropped.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException      (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> dropCollection(CollectionSpec collectionSpec, DropCollectionOptions options) {
    return coreCollectionManager.dropCollection(collectionSpec.scopeName(), collectionSpec.name(), options.build());
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} once the scope is dropped.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public CompletableFuture<Void> dropScope(String scopeName) {
    return dropScope(scopeName, dropScopeOptions());
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} once the scope is dropped.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public CompletableFuture<Void> dropScope(String scopeName, DropScopeOptions options) {
    return coreCollectionManager.dropScope(scopeName, options.build());
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @deprecated use {@link #getAllScopes()} instead.
   */
  @Deprecated
  public CompletableFuture<ScopeSpec> getScope(String scopeName) {
    return getScope(scopeName, getScopeOptions());
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @deprecated use {@link #getAllScopes(GetAllScopesOptions)} instead.
   */
  @Deprecated
  public CompletableFuture<ScopeSpec> getScope(String scopeName, GetScopeOptions options) {
    GetScopeOptions.Built opts = options.build();
    GetAllScopesOptions toPassOptions = getAllScopesOptions();
    opts.timeout().ifPresent(toPassOptions::timeout);
    opts.retryStrategy().ifPresent(toPassOptions::retryStrategy);

    return getAllScopes(toPassOptions).thenApply(scopes -> {
      Optional<ScopeSpec> scope = scopes.stream().filter(s -> s.name().equals(scopeName)).findFirst();
      if (scope.isPresent()) {
        return scope.get();
      } else {
        throw ScopeNotFoundException.forScope(scopeName);
      }
    });
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a {@link CompletableFuture} with a list of scopes in the bucket.
   */
  public CompletableFuture<List<ScopeSpec>> getAllScopes() {
    return getAllScopes(getAllScopesOptions());
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a {@link CompletableFuture} with a list of scopes in the bucket.
   */
  public CompletableFuture<List<ScopeSpec>> getAllScopes(GetAllScopesOptions options) {
    return coreCollectionManager.getAllScopes(options.build())
        // Note that because ns_server returns a different manifest format, we need to do some munching to
        // turn this into the same collections manifest format for sanity.
        .thenApply(manifest ->
            manifest.scopes().stream()
                .map(s -> ScopeSpec.create(
                    s.name(),
                    s.collections().stream()
                        .map(c -> CollectionSpec.create(c.name(), s.name(), Duration.ofSeconds(c.maxExpiry())))
                        .collect(Collectors.toSet()))
                )
                .collect(Collectors.toList()));
  }
}
