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
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.manager.ManagerSupport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
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
public class AsyncCollectionManager extends ManagerSupport {

  private final String bucketName;

  public AsyncCollectionManager(final Core core, final String bucketName) {
    super(core);
    this.bucketName = bucketName;
  }

  private static String pathForScope(final String bucketName, final String scopeName) {
    return pathForManifest(bucketName) + "/" + urlEncode(scopeName);
  }

  private static String pathForCollection(final String bucketName, final String scopeName,
                                          final String collectionName) {
    return pathForScope(bucketName, scopeName) + "/" + urlEncode(collectionName);
  }

  private static String pathForManifest(final String bucketName) {
    return "/pools/default/buckets/" + urlEncode(bucketName) + "/collections";
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection creation completed.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec) {
    return createCollection(collectionSpec, createCollectionOptions());
  }

  /**
   * Creates a collection if it does not already exist.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection creation completed.
   * @throws CollectionExistsException (async) if the collection already exists
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec, final CreateCollectionOptions options) {
    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", collectionSpec.name());

    if (!collectionSpec.maxExpiry().isZero()) {
      body.add("maxTTL", collectionSpec.maxExpiry().getSeconds());
    }

    final String path = pathForScope(bucketName, collectionSpec.scopeName());

    return sendRequest(HttpMethod.POST, path, body, options.build()).thenApply(response -> {
      checkForErrors(response, collectionSpec.scopeName(), collectionSpec.name());
      return null;
    });
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link CompletableFuture} once the scope creation completed.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public CompletableFuture<Void> createScope(final String scopeName) {
    return createScope(scopeName, createScopeOptions());
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeName the name of the scope to create.
   * @return a {@link CompletableFuture} once the scope creation completed.
   * @throws ScopeExistsException (async) if the scope already exists.
   */
  public CompletableFuture<Void> createScope(final String scopeName, final CreateScopeOptions options) {
    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", scopeName);
    final String path = pathForManifest(bucketName);

    return sendRequest(HttpMethod.POST, path, body, options.build()).thenApply(response -> {
        checkForErrors(response, scopeName, null);
        return null;
      });
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection is dropped.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec) {
    return dropCollection(collectionSpec, dropCollectionOptions());
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection is dropped.
   * @throws CollectionNotFoundException (async) if the collection did not exist.
   * @throws ScopeNotFoundException (async) if the specified scope does not exist.
   */
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec, final DropCollectionOptions options) {
    final String path = pathForCollection(bucketName, collectionSpec.scopeName(), collectionSpec.name());
    return sendRequest(HttpMethod.DELETE, path, options.build()).thenApply(response -> {
      checkForErrors(response, collectionSpec.scopeName(), collectionSpec.name());
      return null;
    });
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} once the scope is dropped.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public CompletableFuture<Void> dropScope(final String scopeName) {
    return dropScope(scopeName, dropScopeOptions());
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} once the scope is dropped.
   * @throws ScopeNotFoundException (async) if the scope did not exist.
   */
  public CompletableFuture<Void> dropScope(final String scopeName, final DropScopeOptions options) {
    return sendRequest(HttpMethod.DELETE, pathForScope(bucketName, scopeName), options.build()).thenApply(response -> {
      checkForErrors(response, scopeName, null);
      return null;
    });
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   */
  public CompletableFuture<ScopeSpec> getScope(final String scopeName) {
    return getScope(scopeName, getScopeOptions());
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing information about the scope.
   * @throws ScopeNotFoundException (async) if scope does not exist.
   */
  public CompletableFuture<ScopeSpec> getScope(final String scopeName, final GetScopeOptions options) {
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
  public CompletableFuture<List<ScopeSpec>> getAllScopes(final GetAllScopesOptions options) {
    return loadManifest(options.build()).thenApply(manifest ->
      manifest
        .scopes()
        .stream()
        .map(s -> ScopeSpec.create(
          s.name(),
          s.collections().stream().map(c -> CollectionSpec.create(c.name(), s.name(), Duration.ofSeconds(c.maxExpiry()))).collect(Collectors.toSet()))
        )
        .collect(Collectors.toList()));
  }

  /**
   * Helper method to check for common errors and raise the right exceptions in those cases.
   *
   * @param response the response to check.
   */
  private void checkForErrors(final GenericManagerResponse response, final String scopeName, final String collectionName) {
    if (response.status().success()) {
      return;
    }
    final String error = new String(response.content(), StandardCharsets.UTF_8);

    if (response.status() == ResponseStatus.NOT_FOUND) {
      if (error.contains("Scope with this name is not found")) {
        throw ScopeNotFoundException.forScope(scopeName);
      }

      if (error.contains("Collection with this name is not found")) {
        throw CollectionNotFoundException.forCollection(collectionName);
      }
    }

    if (response.status() == ResponseStatus.INVALID_ARGS) {
      if (error.contains("Scope with this name already exists")) {
        throw ScopeExistsException.forScope(scopeName);
      }
      if (error.contains("Collection with this name already exists")) {
        throw CollectionExistsException.forCollection(collectionName);
      }
    }

    throw new CouchbaseException("Unknown error in CollectionManager: " + error);
  }

  /**
   * Note that because ns_server returns a different manifest format, we need to do some munching to
   * turn this into the same collections manifest format for sanity.
   *
   * @return the loaded manifest.
   */
  private CompletableFuture<CollectionsManifest> loadManifest(final CommonOptions<?>.BuiltCommonOptions opts) {
    return sendRequest(HttpMethod.GET, pathForManifest(bucketName), opts)
      .thenApply(response -> Mapper.decodeInto(response.content(), CollectionsManifest.class));
  }

}
