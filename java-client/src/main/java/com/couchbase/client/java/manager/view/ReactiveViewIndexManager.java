/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.manager.view;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.view.DesignDocumentNamespace;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

public class ReactiveViewIndexManager {

  private final AsyncViewIndexManager async;

  public ReactiveViewIndexManager(AsyncViewIndexManager async) {
    this.async = requireNonNull(async);
  }

  /**
   * Returns the design document from the cluster if present.
   *
   * @param name name of the design document to retrieve.
   * @param namespace namespace to look in.
   * @throws DesignDocumentNotFoundException (async) if there is no design document with the given name present.
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<DesignDocument> getDesignDocument(String name, DesignDocumentNamespace namespace) {
    return Reactor.toMono(() -> async.getDesignDocument(name, namespace));
  }

  /**
   * Returns the design document from the cluster if present with custom options.
   *
   * @param name name of the design document to retrieve
   * @param namespace namespace to look in
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException (async) if there is no design document with the given name present.
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<DesignDocument> getDesignDocument(String name, DesignDocumentNamespace namespace, GetDesignDocumentOptions options) {
    return Reactor.toMono(() -> async.getDesignDocument(name, namespace, options));
  }

  /**
   * Stores the design document on the server under the specified namespace, replacing any existing document
   * with the same name.
   *
   * @param designDocument document to store
   * @param namespace namespace to store it in
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> upsertDesignDocument(DesignDocument designDocument, DesignDocumentNamespace namespace) {
    return Reactor.toMono(() -> async.upsertDesignDocument(designDocument, namespace));
  }

  /**
   * Stores the design document on the server under the specified namespace, replacing any existing document
   * with the same name.
   *
   * @param designDocument document to store
   * @param namespace namespace to store it in
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> upsertDesignDocument(DesignDocument designDocument, DesignDocumentNamespace namespace, UpsertDesignDocumentOptions options) {
    return Reactor.toMono(() -> async.upsertDesignDocument(designDocument, namespace, options));
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @throws DesignDocumentNotFoundException (async) if the development namespace does not contain a document with the given name
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> publishDesignDocument(String name) {
    return Reactor.toMono(() -> async.publishDesignDocument(name));
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException (async) if the development namespace does not contain a document with the given name
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> publishDesignDocument(String name, PublishDesignDocumentOptions options) {
    return Reactor.toMono(() -> async.publishDesignDocument(name, options));
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @param namespace namespace to remove it from
   * @throws DesignDocumentNotFoundException (async) if the namespace does not contain a document with the given name
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> dropDesignDocument(String name, DesignDocumentNamespace namespace) {
    return Reactor.toMono(() -> async.dropDesignDocument(name, namespace));
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @param namespace namespace to remove it from
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException (async) if the namespace does not contain a document with the given name
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<Void> dropDesignDocument(String name, DesignDocumentNamespace namespace, DropDesignDocumentOptions options) {
    return Reactor.toMono(() -> async.dropDesignDocument(name, namespace, options));
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Flux<DesignDocument> getAllDesignDocuments(DesignDocumentNamespace namespace) {
    return Reactor.toFlux(() -> async.getAllDesignDocuments(namespace));
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws TimeoutException (async) if the operation times out before getting a result.
   * @throws CouchbaseException (async) for all other error reasons (acts as a base type and catch-all).
   */
  public Flux<DesignDocument> getAllDesignDocuments(DesignDocumentNamespace namespace, GetAllDesignDocumentsOptions options) {
    return Reactor.toFlux(() -> async.getAllDesignDocuments(namespace, options));
  }
}
