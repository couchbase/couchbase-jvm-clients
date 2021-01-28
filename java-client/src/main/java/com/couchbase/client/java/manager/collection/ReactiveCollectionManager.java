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
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.Reactor.toMono;
import static com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions;
import static com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions;
import static com.couchbase.client.java.manager.collection.DropCollectionOptions.dropCollectionOptions;
import static com.couchbase.client.java.manager.collection.DropScopeOptions.dropScopeOptions;
import static com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions;
import static com.couchbase.client.java.manager.collection.GetScopeOptions.getScopeOptions;

/**
 * The {@link ReactiveCollectionManager} provides APIs to manage bucket collections and scopes.
 */
@Stability.Volatile
public class ReactiveCollectionManager {

  private final AsyncCollectionManager async;

  public ReactiveCollectionManager(AsyncCollectionManager async) {
    this.async = async;
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public Mono<Void> createCollection(final CollectionSpec collectionSpec, final CreateCollectionOptions options) {
    return toMono(() -> async.createCollection(collectionSpec, options));
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public Mono<Void> dropCollection(final CollectionSpec collectionSpec, final DropCollectionOptions options) {
    return toMono(() -> async.dropCollection(collectionSpec, options));
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public Mono<Void> createScope(final String scopeName, final CreateScopeOptions options) {
    return toMono(() -> async.createScope(scopeName, options));
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public Mono<Void> dropScope(final String scopeName, final DropScopeOptions options) {
    return toMono(() -> async.dropScope(scopeName, options));
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a mono containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @deprecated use {@link #getAllScopes(GetAllScopesOptions)} instead.
   */
  @Deprecated
  public Mono<ScopeSpec> getScope(final String scopeName, final GetScopeOptions options) {
    return toMono(() -> async.getScope(scopeName, options));
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a flux of all scopes in this bucket.
   */
  public Flux<ScopeSpec> getAllScopes(final GetAllScopesOptions options) {
    return toMono(() -> async.getAllScopes(options)).flatMapMany(Flux::fromIterable);
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public Mono<Void> createCollection(final CollectionSpec collectionSpec) {
    return createCollection(collectionSpec, createCollectionOptions());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public Mono<Void> dropCollection(final CollectionSpec collectionSpec) {
    return dropCollection(collectionSpec, dropCollectionOptions());
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public Mono<Void> createScope(final String scopeName) {
    return createScope(scopeName, createScopeOptions());
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public Mono<Void> dropScope(final String scopeName) {
    return dropScope(scopeName, dropScopeOptions());
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a mono containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   * @deprecated use {@link #getAllScopes()} instead.
   */
  @Deprecated
  public Mono<ScopeSpec> getScope(final String scopeName) {
    return getScope(scopeName, getScopeOptions());
  }

  /**
   * Returns all scopes in this bucket.
   *
   * @return a flux of all scopes in this bucket.
   */
  public Flux<ScopeSpec> getAllScopes() {
    return getAllScopes(getAllScopesOptions());
  }

}
