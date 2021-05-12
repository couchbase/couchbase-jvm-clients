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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
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

  private static String pathForScopes(final String bucketName) {
    return "/pools/default/buckets/" + urlEncode(bucketName) + "/scopes";
  }

  private static String pathForScope(final String bucketName, final String scopeName) {
    return pathForScopes(bucketName) + "/" + urlEncode(scopeName);
  }

  private static String pathForCollections(final String bucketName, final String scopeName) {
    return pathForScope(bucketName, scopeName) + "/collections";
  }

  private static String pathForCollection(final String bucketName, final String scopeName,
                                          final String collectionName) {
    return pathForCollections(bucketName, scopeName) + "/" + urlEncode(collectionName);
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
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec,
                                                  final CreateCollectionOptions options) {
    CreateCollectionOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION,
      built.parentSpan().orElse(null),
      bucketName,
      collectionSpec.scopeName(),
      collectionSpec.name()
    );

    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", collectionSpec.name());

    if (!collectionSpec.maxExpiry().isZero()) {
      body.add("maxTTL", collectionSpec.maxExpiry().getSeconds());
    }

    final String path = pathForCollections(bucketName, collectionSpec.scopeName());

    return sendRequest(HttpMethod.POST, path, body, built, span).thenApply(response -> {
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
    CreateScopeOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE,
      built.parentSpan().orElse(null),
      bucketName,
      scopeName,
      null
    );

    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", scopeName);
    final String path = pathForScopes(bucketName);

    return sendRequest(HttpMethod.POST, path, body, built, span).thenApply(response -> {
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
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec,
                                                final DropCollectionOptions options) {
    DropCollectionOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION,
      built.parentSpan().orElse(null),
      bucketName,
      collectionSpec.scopeName(),
      collectionSpec.name()
    );

    final String path = pathForCollection(bucketName, collectionSpec.scopeName(), collectionSpec.name());
    return sendRequest(HttpMethod.DELETE, path, built, span).thenApply(response -> {
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
    DropScopeOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE,
      built.parentSpan().orElse(null),
      bucketName,
      scopeName,
      null
    );

    return sendRequest(HttpMethod.DELETE, pathForScope(bucketName, scopeName), built, span).thenApply(response -> {
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
   * @deprecated use {@link #getAllScopes()} instead.
   */
  @Deprecated
  public CompletableFuture<ScopeSpec> getScope(final String scopeName) {
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
    GetAllScopesOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES,
      built.parentSpan().orElse(null),
      bucketName,
      null,
      null
    );

    return loadManifest(built, span).thenApply(manifest ->
      manifest
        .scopes()
        .stream()
        .map(s -> ScopeSpec.create(
          s.name(),
          s
            .collections()
            .stream()
            .map(c -> CollectionSpec.create(c.name(), s.name(), Duration.ofSeconds(c.maxExpiry())))
            .collect(Collectors.toSet()))
        )
        .collect(Collectors.toList()));
  }

  /**
   * Helper method to check for common errors and raise the right exceptions in those cases.
   *
   * @param response the response to check.
   */
  private void checkForErrors(final GenericManagerResponse response, final String scopeName,
                              final String collectionName) {
    if (response.status().success()) {
      return;
    }

    final String error = new String(response.content(), StandardCharsets.UTF_8);

    if (response.status() == ResponseStatus.NOT_FOUND) {
      if (error.contains("Not found.") || error.contains("Requested resource not found.")) {
        // This happens on pre 6.5 clusters (i.e. 5.5)
        throw FeatureNotAvailableException.collections();
      }

      if (error.matches(".*Scope.+not found.*") || error.contains("scope_not_found")) {
        throw ScopeNotFoundException.forScope(scopeName);
      }

      if (error.matches(".*Collection.+not found.*")) {
        throw CollectionNotFoundException.forCollection(collectionName);
      }
    }

    if (response.status() == ResponseStatus.INVALID_ARGS) {
      if (error.matches(".*Scope.+already exists.*")) {
        throw ScopeExistsException.forScope(scopeName);
      }
      if (error.contains("scope_not_found")) {
        throw ScopeNotFoundException.forScope(scopeName);
      }
      if (error.matches(".*Collection.+already exists.*")) {
        throw CollectionExistsException.forCollection(collectionName);
      }
      if (error.contains("Not allowed on this version of cluster")) {
        // This happens on 6.5 if collections dev preview is not enabled
        throw FeatureNotAvailableException.collections();
      }
      if (error.matches(".*Collection.+not found.*")
        || error.contains("collection_not_found")) {
        throw CollectionNotFoundException.forCollection(collectionName);
      }
    }

    if (error.contains("Method Not Allowed")) {
      // Happens on pre 6.5 clusters on i.e. dropScope
      throw FeatureNotAvailableException.collections();
    }

    throw new CouchbaseException("Unknown error in CollectionManager: " + error + ", response: " + response);
  }

  /**
   * Note that because ns_server returns a different manifest format, we need to do some munching to
   * turn this into the same collections manifest format for sanity.
   *
   * @return the loaded manifest.
   */
  private CompletableFuture<CollectionsManifest> loadManifest(final CommonOptions<?>.BuiltCommonOptions opts,
                                                              RequestSpan span) {
    return sendRequest(HttpMethod.GET, pathForScopes(bucketName), opts, span)
      .thenApply(response -> {
        checkForErrors(response, null, null);
        return Mapper.decodeInto(response.content(), CollectionsManifest.class);
      });
  }

  private RequestSpan buildSpan(final String spanName, final RequestSpan parent, final String bucketName,
                                final String scopeName, final String collectionName) {
    RequestSpan span = environment().requestTracer().requestSpan(spanName, parent);
    if (bucketName != null) {
      span.attribute(TracingIdentifiers.ATTR_NAME, bucketName);
    }
    if (scopeName != null) {
      span.attribute(TracingIdentifiers.ATTR_SCOPE, scopeName);
    }
    if (collectionName != null) {
      span.attribute(TracingIdentifiers.ATTR_COLLECTION, collectionName);
    }
    return span;
  }

}
