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
import com.couchbase.client.core.util.ReactorOps;
import com.couchbase.client.java.ReactiveBucket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions;
import static com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions;
import static com.couchbase.client.java.manager.collection.DropCollectionOptions.dropCollectionOptions;
import static com.couchbase.client.java.manager.collection.DropScopeOptions.dropScopeOptions;
import static com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions;
import static com.couchbase.client.java.manager.collection.GetScopeOptions.getScopeOptions;
import static java.util.Objects.requireNonNull;

/**
 * The {@link ReactiveCollectionManager} provides APIs to manage collections and scopes within a bucket.
 */
@Stability.Volatile
public class ReactiveCollectionManager {

  /**
   * The underlying async collection manager.
   */
  private final AsyncCollectionManager async;
  private final ReactorOps reactor;

  /**
   * Creates a new {@link ReactiveCollectionManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link ReactiveBucket#collections()}
   * instead.
   *
   * @param async the underlying async collection manager.
   */
  @Stability.Internal
  public ReactiveCollectionManager(final ReactorOps reactor, final AsyncCollectionManager async) {
    this.reactor = requireNonNull(reactor);
    this.async = requireNonNull(async);
  }

  /**
   * Creates a collection if it does not already exist.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated This method cannot be used to set the collection's "history" property.
   * This method is not compatible with Couchbase Server Community Edition.
   * Please use {@link #createCollection(String, String, CreateCollectionSettings)} instead.
   */
  @Deprecated
  public Mono<Void> createCollection(final CollectionSpec collectionSpec) {
    return createCollection(collectionSpec, createCollectionOptions());
  }

  /**
   * Creates a collection if it does not already exist with custom options.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated This method cannot be used to set the collection's "history" property.
   * This method is not compatible with Couchbase Server Community Edition.
   * Please use {@link #createCollection(String, String, CreateCollectionSettings, CreateCollectionOptions)} instead.
   */
  @Deprecated
  public Mono<Void> createCollection(final CollectionSpec collectionSpec, final CreateCollectionOptions options) {
    return reactor.publishOnUserScheduler(() -> async.createCollection(collectionSpec, options));
  }

  /**
   * Creates a collection if it does not already exist with custom options.
   * <p>
   * Note that a scope needs to be created first (via {@link #createScope(String)}) if it doesn't exist already.
   *
   * @param scopeName name of scope to create collection in
   * @param collectionName name of collection to create
   * @param settings the collection settings
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> createCollection(final String scopeName, final String collectionName, final CreateCollectionSettings settings) {
    return reactor.publishOnUserScheduler(() -> async.createCollection(scopeName, collectionName, settings));
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
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionExistsException (async) if the collection already exists.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> createCollection(final String scopeName, final String collectionName, final CreateCollectionSettings settings, final CreateCollectionOptions options) {
    return reactor.publishOnUserScheduler(() -> async.createCollection(scopeName, collectionName, settings, options));
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws ScopeExistsException (async) if the scope already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createScope(final String scopeName) {
    return createScope(scopeName, createScopeOptions());
  }

  /**
   * Creates a scope if it does not already exist with custom options.
   *
   * @param scopeName the name of the scope to create.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws ScopeExistsException (async) if the scope already exists.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createScope(final String scopeName, final CreateScopeOptions options) {
    return reactor.publishOnUserScheduler(() -> async.createScope(scopeName, options));
  }

  /**
   * Updates a collection with custom options.
   *
   * @param scopeName name of scope to update collection in
   * @param collectionName name of collection to update
   * @param settings the collection settings
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the specified collection does not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> updateCollection(String scopeName, String collectionName, UpdateCollectionSettings settings) {
    return reactor.publishOnUserScheduler(() -> async.updateCollection(scopeName, collectionName, settings));
  }

  /**
   * Updates a collection with custom options.
   *
   * @param scopeName name of scope to update collection in
   * @param collectionName name of collection to update
   * @param settings the collection settings
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the specified collection does not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> updateCollection(String scopeName, String collectionName, UpdateCollectionSettings settings, UpdateCollectionOptions options) {
    return reactor.publishOnUserScheduler(() -> async.updateCollection(scopeName, collectionName, settings, options));
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated Please use {@link #dropCollection(String, String)} instead.
   */
  @Deprecated
  public Mono<Void> dropCollection(final CollectionSpec collectionSpec) {
    return dropCollection(collectionSpec, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists with custom options.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated Please use {@link #dropCollection(String, String, DropCollectionOptions)} instead.
   */
  @Deprecated
  public Mono<Void> dropCollection(final CollectionSpec collectionSpec, final DropCollectionOptions options) {
    return reactor.publishOnUserScheduler(() -> async.dropCollection(collectionSpec, options));
  }

  /**
   * Drops a collection if it exists.
   *
   * @param scopeName name of scope to drop collection from
   * @param collectionName name of collection to drop
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> dropCollection(final String scopeName, final String collectionName) {
    return dropCollection(scopeName, collectionName, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists with custom options.
   *
   * @param scopeName name of scope to drop collection from
   * @param collectionName name of collection to drop
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  @Stability.Volatile
  public Mono<Void> dropCollection(final String scopeName, final String collectionName, final DropCollectionOptions options) {
    return reactor.publishOnUserScheduler(() -> async.dropCollection(scopeName, collectionName, options));
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropScope(final String scopeName) {
    return dropScope(scopeName, dropScopeOptions());
  }

  /**
   * Drops a scope if it exists with custom options.
   *
   * @param scopeName the name of the scope to drop.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropScope(final String scopeName, final DropScopeOptions options) {
    return reactor.publishOnUserScheduler(() -> async.dropScope(scopeName, options));
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link Mono} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated use {@link #getAllScopes()} instead.
   */
  @Deprecated
  public Mono<ScopeSpec> getScope(final String scopeName) {
    return getScope(scopeName, getScopeOptions());
  }

  /**
   * Returns the scope if it exists with custom options.
   *
   * @param scopeName the name of the scope.
   * @param options the custom options to apply.
   * @return a {@link Mono} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   * @deprecated use {@link #getAllScopes(GetAllScopesOptions)} instead.
   */
  @Deprecated
  public Mono<ScopeSpec> getScope(final String scopeName, final GetScopeOptions options) {
    return reactor.publishOnUserScheduler(() -> async.getScope(scopeName, options));
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a {@link Flux} with a (potentially empty) list of scopes in the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<ScopeSpec> getAllScopes() {
    return getAllScopes(getAllScopesOptions());
  }

  /**
   * Returns all scopes in this bucket with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Flux} with a (potentially empty) list of scopes in the bucket.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<ScopeSpec> getAllScopes(final GetAllScopesOptions options) {
    return reactor.publishOnUserScheduler(() -> async.getAllScopes(options)).flatMapMany(Flux::fromIterable);
  }

}
