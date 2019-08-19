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
import com.couchbase.client.core.config.CollectionsManifestCollection;
import com.couchbase.client.core.config.CollectionsManifestScope;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.json.MapperException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.manager.ManagerSupport;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;

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
   */
  public CompletableFuture<Void> createCollection(final CollectionSpec collectionSpec) {
    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", collectionSpec.name());
    final String path = pathForScope(bucketName, collectionSpec.scopeName());

    return sendRequest(HttpMethod.POST, path, body).thenApply(response -> {
      checkForErrors(response, collectionSpec.scopeName(), collectionSpec.name());
      return null;
    });
  }

  /**
   * Checks if the given collection exists in this bucket.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} returning true if it exists, false otherwise.
   */
  public CompletableFuture<Boolean> collectionExists(final CollectionSpec collectionSpec) {
    return loadManifest().thenApply(manifest -> {
      for (CollectionsManifestScope scope : manifest.scopes()) {
        if (scope.name().equals(collectionSpec.scopeName())) {
          for (CollectionsManifestCollection collection : scope.collections()) {
            if (collection.name().equals(collectionSpec.name())) {
              return true;
            }
          }
        }
      }
      return false;
    });
  }

  /**
   * Creates a scope if it does not already exist.
   *
   * @param scopeSpec the scope spec that contains the properties of the scope.
   * @return a {@link CompletableFuture} once the scope creation completed.
   */
  public CompletableFuture<Void> createScope(final ScopeSpec scopeSpec) {
    final UrlQueryStringBuilder body = UrlQueryStringBuilder
      .create()
      .add("name", scopeSpec.name());
    final String path = pathForManifest(bucketName);

    return Mono
      .fromFuture(sendRequest(HttpMethod.POST, path, body).thenApply(response -> {
        checkForErrors(response, scopeSpec.name(), null);
        return null;
      }))
      .then(Mono.defer(() -> {
        List<Mono<Void>> collectionsCreated = scopeSpec
          .collections()
          .stream()
          .map(s -> Mono.fromFuture(createCollection(s)))
          .collect(Collectors.toList());

        return collectionsCreated.isEmpty() ? Mono.empty() : Mono.when(collectionsCreated);
      }))
      .toFuture();
  }

  /**
   * Drops a collection if it exists.
   *
   * @param collectionSpec the collection spec that contains the properties of the collection.
   * @return a {@link CompletableFuture} once the collection is dropped.
   */
  public CompletableFuture<Void> dropCollection(final CollectionSpec collectionSpec) {
    final String path = pathForCollection(bucketName, collectionSpec.scopeName(), collectionSpec.name());
    return sendRequest(HttpMethod.DELETE, path).thenApply(response -> {
      checkForErrors(response, collectionSpec.scopeName(), collectionSpec.name());
      return null;
    });
  }

  /**
   * Drops a scope if it exists.
   *
   * @param scopeName the name of the scope to drop.
   * @return a {@link CompletableFuture} once the scope is dropped.
   */
  public CompletableFuture<Void> dropScope(final String scopeName) {
    return sendRequest(HttpMethod.DELETE, pathForScope(bucketName, scopeName)).thenApply(response -> {
      checkForErrors(response, scopeName, null);
      return null;
    });
  }

  /**
   * Returns the scope if it exists.
   *
   * @param scopeName the name of the scope.
   * @return a {@link CompletableFuture} containing the scope spec if it exists.
   */
  public CompletableFuture<ScopeSpec> getScope(final String scopeName) {
    return getAllScopes().thenApply(scopes -> {
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
    return loadManifest().thenApply(manifest ->
      manifest
        .scopes()
        .stream()
        .map(s -> ScopeSpec.create(
          s.name(),
          s.collections().stream().map(c -> CollectionSpec.create(c.name(), s.name())).collect(Collectors.toSet()))
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
      if (error.contains("Scope with this name is not found")) {
        throw ScopeNotFoundException.forScope(scopeName);
      }

      if (error.contains("Collection with this name is not found")) {
        throw CollectionNotFoundException.forCollection(collectionName);
      }
    }

    if (response.status() == ResponseStatus.INVALID_ARGS) {
      if (error.contains("Scope with this name already exists")) {
        throw ScopeAlreaadyExistsException.forScope(scopeName);
      }
      if (error.contains("Collection with this name already exists")) {
        throw CollectionAlreadyExistsException.forCollection(collectionName);
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
  private CompletableFuture<CollectionsManifest> loadManifest() {
    return sendRequest(HttpMethod.GET, pathForManifest(bucketName)).thenApply(response -> {
      try {
        return Mapper.decodeInto(response.content(), CollectionsManifest.class);
      } catch (MapperException ex) {
        // TODO: this needs to go away after mad hatter beta 2 is released.
        return decodeBetaFallback(response.content());
      }
    });
  }

  /**
   * This needs to go away after we ship Mad Hatter Beta 2, since this code is purely here so that the
   * client keeps working with beta 1.
   *
   * <p>TODO: Remove me</p>
   */
  @SuppressWarnings("unchecked")
  private CollectionsManifest decodeBetaFallback(final byte[] content) {
    Map<String, Object> decoded = Mapper.decodeInto(content, new TypeReference<Map<String, Object>>() {});
    int uid = (int) decoded.get("uid");

    List<CollectionsManifestScope> scopes = new ArrayList<>();
    for (Map.Entry<String, Map<String, Object>> scope : ((Map<String, Map<String, Object>>) decoded.get("scopes")).entrySet()) {
      String name = scope.getKey();
      int scopeUid = (int) scope.getValue().get("uid");

      List<CollectionsManifestCollection> collections = new ArrayList<>();
      for (Map.Entry<String, Map<String, Object>> collection : ((Map<String, Map<String, Object>>) scope.getValue().get("collections")).entrySet()) {
        String collectionName = collection.getKey();
        int collectionUid = (int) collection.getValue().get("uid");
        collections.add(new CollectionsManifestCollection(collectionName, Long.toString(collectionUid)));
      }
      scopes.add(new CollectionsManifestScope(name, Long.toString(scopeUid), collections));
    }

    return new CollectionsManifest(Long.toString(uid), scopes);
  }

}
