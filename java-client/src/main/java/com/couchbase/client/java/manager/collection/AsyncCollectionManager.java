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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.manager.CoreCollectionManager;
import com.couchbase.client.core.manager.collection.CoreCreateOrUpdateCollectionSettings;
import com.couchbase.client.core.util.PreventsGarbageCollection;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.AsyncCluster;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.manager.collection.CreateCollectionSettings.createCollectionSettings;
import static com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions;
import static com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions;
import static com.couchbase.client.java.manager.collection.DropCollectionOptions.dropCollectionOptions;
import static com.couchbase.client.java.manager.collection.DropScopeOptions.dropScopeOptions;
import static com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions;
import static com.couchbase.client.java.manager.collection.GetScopeOptions.getScopeOptions;
import static com.couchbase.client.java.manager.collection.UpdateCollectionOptions.updateCollectionOptions;
import static java.util.Objects.requireNonNull;

/**
 * The {@link AsyncCollectionManager} provides APIs to manage collections and scopes within a bucket.
 */
@Stability.Volatile
public class AsyncCollectionManager {

  /**
   * References the core-io collection manager which abstracts common I/O functionality.
   */
  private final CoreCollectionManager coreCollectionManager;

  @PreventsGarbageCollection
  private final AsyncCluster cluster;

  /**
   * Creates a new {@link AsyncCollectionManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncBucket#collections()}
   * instead.
   */
  @Stability.Internal
  public AsyncCollectionManager(
    final CoreCollectionManager coreCollectionManager,
    final AsyncCluster cluster
  ) {
      this.coreCollectionManager = requireNonNull(coreCollectionManager);
      this.cluster = requireNonNull(cluster);
   }

  /**
   * Creates a collection if it does not already exist.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated This method cannot be used to set the collection's "history" property.
   * Please use {@link #createCollection(String, String, CreateCollectionSettings)} instead.
   */
  @Deprecated
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec) {
    return createCollection(collectionSpec.scopeName(), collectionSpec.name(), CreateCollectionSettings.createCollectionSettings());
  }

  /**
   * Creates a collection if it does not already exist with custom options.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated This method cannot be used to set the collection's "history" property.
   * Please use {@link #createCollection(String, String, CreateCollectionSettings, CreateCollectionOptions)} instead.
   */
  @Deprecated
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec,
                                                  final CreateCollectionOptions options) {
    return coreCollectionManager.createCollection(collectionSpec.scopeName(), collectionSpec.name(),
      new CoreCreateOrUpdateCollectionSettings() {
        @Override
        public Duration maxExpiry() {
          return collectionSpec.maxExpiry();
        }

        @Override
        public Boolean history() {
          return collectionSpec.history();
        }
      }, options.build());
  }

  /**
   * Creates a collection if it does not already exist with custom options.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param scopeName name of scope to create collection in
   * @param collectionName name of collection to create
   * @param settings the collection settings
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> createCollection(String scopeName, String collectionName, CreateCollectionSettings settings) {
    return coreCollectionManager.createCollection(scopeName, collectionName, settings.build(), createCollectionOptions().build());
  }

  /**
   * Creates a collection if it does not already exist with custom options.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param scopeName name of scope to create collection in
   * @param collectionName name of collection to create
   * @param settings the collection settings
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> createCollection(String scopeName, String collectionName, CreateCollectionSettings settings, CreateCollectionOptions options) {
    return coreCollectionManager.createCollection(scopeName, collectionName, settings.build(), options.build());
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws ScopeExistsException (async) if the scope already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createScope(final String scopeName) {
    return createScope(scopeName, createScopeOptions());
  }

  /**
   * Creates a scope if it does not already exist with custom options.
   *
   * @param scopeName the name of the scope to create.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws ScopeExistsException (async) if the scope already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createScope(final String scopeName, final CreateScopeOptions options) {
    return coreCollectionManager.createScope(scopeName, options.build());
  }

  /**
   * Updates a collection with custom options.
   *
   * @param scopeName name of scope to update collection in
   * @param collectionName name of collection to update
   * @param settings the collection settings
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> updateCollection(final String scopeName, final String collectionName, final UpdateCollectionSettings settings) {
    return updateCollection(scopeName, collectionName, settings, updateCollectionOptions());
  }

  /**
   * Updates a collection with custom options.
   *
   * @param scopeName name of scope to update collection in
   * @param collectionName name of collection to update
   * @param settings the collection settings
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection does not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> updateCollection(final String scopeName, final String collectionName, final UpdateCollectionSettings settings, final UpdateCollectionOptions options) {
    return coreCollectionManager.updateCollection(scopeName, collectionName, settings.build(), options.build());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection does not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated Please use {@link #dropCollection(String, String)} instead.
   */
  @Deprecated
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec) {
    return dropCollection(collectionSpec, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists with custom options.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated Please use {@link #dropCollection(String, String, DropCollectionOptions)} instead.
   */
  @Deprecated
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec,
                                                final DropCollectionOptions options) {
    return coreCollectionManager.dropCollection(collectionSpec.scopeName(), collectionSpec.name(), options.build());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param scopeName name of scope to drop collection from
   * @param collectionName name of collection to drop
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> dropCollection(final String scopeName,
                                                final String collectionName) {
    return dropCollection(scopeName, collectionName, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists with custom options.
   *
   * @param scopeName name of scope to drop collection from
   * @param collectionName name of collection to drop
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public CompletableFuture<Void> dropCollection(final String scopeName,
                                                final String collectionName,
                                                final DropCollectionOptions options) {
    return coreCollectionManager.dropCollection(scopeName, collectionName, options.build());
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropScope(final String scopeName) {
    return dropScope(scopeName, dropScopeOptions());
  }

  /**
   * Drops a scope if it exists with custom options.
   *
   * @param scopeName the name of the scope to drop.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropScope(final String scopeName, final DropScopeOptions options) {
    return coreCollectionManager.dropScope(scopeName, options.build());
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated use {@link #getAllScopes()} instead.
   */
  @Deprecated
  public CompletableFuture<ScopeSpec> getScope(final String scopeName) {
    return getScope(scopeName, getScopeOptions());
  }

  /**
   * Returns the scope if it exists with custom options.
   *
   * @param scopeName the name of the scope.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated use {@link #getAllScopes(GetAllScopesOptions)} instead.
   */
  @Deprecated
  public CompletableFuture<ScopeSpec> getScope(final String scopeName, final GetScopeOptions options) {
    notNullOrEmpty(scopeName, "ScopeName");
    notNull(options, "Options");

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
   * @return a {@link CompletableFuture} with a (potentially empty) list of scopes in the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<ScopeSpec>> getAllScopes() {
    return getAllScopes(getAllScopesOptions());
  }

  /**
   * Returns all scopes in this bucket with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} with a (potentially empty) list of scopes in the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<ScopeSpec>> getAllScopes(final GetAllScopesOptions options) {
    return coreCollectionManager.getAllScopes(options.build())
        // Note that because ns_server returns a different manifest format, we need to do some munching to
        // turn this into the same collections manifest format for sanity.
        .thenApply(manifest -> manifest.scopes().stream()
            .map(s -> ScopeSpec.create(s.name(),
                s.collections().stream()
                    .map(c -> CollectionSpec.internalCreate(c.name(), s.name(), c.maxExpiry() == null ? Duration.ZERO : Duration.ofSeconds(c.maxExpiry()), c.history()))
                    .collect(Collectors.toSet())))
            .collect(Collectors.toList()));
  }
}
