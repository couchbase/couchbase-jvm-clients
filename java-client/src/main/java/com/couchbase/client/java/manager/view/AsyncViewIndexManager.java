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

package com.couchbase.client.java.manager.view;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.context.ReducedViewErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreViewIndexManager;
import com.couchbase.client.java.view.DesignDocumentNamespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.CbStrings.removeStart;
import static com.couchbase.client.core.util.CbThrowables.hasCause;
import static com.couchbase.client.core.util.CbThrowables.throwIfUnchecked;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.manager.view.DropDesignDocumentOptions.dropDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.GetAllDesignDocumentsOptions.getAllDesignDocumentsOptions;
import static com.couchbase.client.java.manager.view.GetDesignDocumentOptions.getDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.PublishDesignDocumentOptions.publishDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.UpsertDesignDocumentOptions.upsertDesignDocumentOptions;
import static com.couchbase.client.java.view.DesignDocumentNamespace.PRODUCTION;
import static java.util.Objects.requireNonNull;

public class AsyncViewIndexManager {

  private final CoreViewIndexManager coreManager;
  private final String bucket;

  public AsyncViewIndexManager(Core core, String bucket) {
    coreManager = new CoreViewIndexManager(core, bucket);
    this.bucket = requireNonNull(bucket);
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   */
  public CompletableFuture<List<DesignDocument>> getAllDesignDocuments(final DesignDocumentNamespace namespace) {
    return getAllDesignDocuments(namespace, getAllDesignDocumentsOptions());
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   * @param options additional optional arguments (timeout, retry, etc.)
   */
  public CompletableFuture<List<DesignDocument>> getAllDesignDocuments(final DesignDocumentNamespace namespace,
                                                                       final GetAllDesignDocumentsOptions options) {
    notNull(namespace, "DesignDocumentNamespace", () -> new ReducedViewErrorContext(null, null, bucket));
    notNull(options, "GetAllDesignDocumentsOptions", () -> new ReducedViewErrorContext(null, null, bucket));

    return coreManager.getAllDesignDocuments(namespace == PRODUCTION, options.build())
        .thenApply(AsyncViewIndexManager::parseAllDesignDocuments);
  }

  private static List<DesignDocument> parseAllDesignDocuments(Map<String, ObjectNode> ddocNameToJson) {
    List<DesignDocument> result = new ArrayList<>();
    ddocNameToJson.forEach((k, v) -> result.add(parseDesignDocument(k, v)));
    return result;
  }

  /**
   * Returns the named design document from the specified namespace.
   *
   * @param name name of the design document to retrieve
   * @param namespace namespace to look in
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<DesignDocument> getDesignDocument(String name, DesignDocumentNamespace namespace) {
    return getDesignDocument(name, namespace, getDesignDocumentOptions());
  }

  /**
   * Returns the named design document from the specified namespace.
   *
   * @param name name of the design document to retrieve
   * @param namespace namespace to look in
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<DesignDocument> getDesignDocument(String name, DesignDocumentNamespace namespace, GetDesignDocumentOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    notNull(namespace, "DesignDocumentNamespace", () -> new ReducedViewErrorContext(name, null, bucket));
    notNull(options, "GetDesignDocumentOptions", () -> new ReducedViewErrorContext(name, null, bucket));

    return coreManager.getDesignDocument(name, namespace == PRODUCTION, options.build())
        .thenApply(responseBytes -> parseDesignDocument(name, Mapper.decodeIntoTree(responseBytes)));
  }

  private static DesignDocument parseDesignDocument(final String name, final JsonNode node) {
    ObjectNode viewsNode = (ObjectNode) node.path("views");
    Map<String, View> views = Mapper.convertValue(viewsNode, new TypeReference<Map<String, View>>() {});
    return new DesignDocument(removeStart(name, "dev_"), views);
  }

  /**
   * Stores the design document on the server under the specified namespace, replacing any existing document
   * with the same name.
   *
   * @param doc document to store
   * @param namespace namespace to store it in
   */
  public CompletableFuture<Void> upsertDesignDocument(DesignDocument doc, DesignDocumentNamespace namespace) {
    return upsertDesignDocument(doc, namespace, upsertDesignDocumentOptions());
  }

  /**
   * Stores the design document on the server under the specified namespace, replacing any existing document
   * with the same name.
   *
   * @param doc document to store
   * @param namespace namespace to store it in
   * @param options additional optional arguments (timeout, retry, etc.)
   */
  public CompletableFuture<Void> upsertDesignDocument(final DesignDocument doc, final DesignDocumentNamespace namespace,
                                                      final UpsertDesignDocumentOptions options) {
    notNull(doc, "DesignDocument", () -> new ReducedViewErrorContext(null, null, bucket));
    notNull(namespace, "DesignDocumentNamespace", () -> new ReducedViewErrorContext(doc.name(), null, bucket));
    notNull(options, "UpsertDesignDocumentOptions", () -> new ReducedViewErrorContext(doc.name(), null, bucket));

    byte[] docBytes = Mapper.encodeAsBytes(toJson(doc));
    return coreManager.upsertDesignDocument(doc.name(), docBytes, namespace == PRODUCTION, options.build());
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @throws DesignDocumentNotFoundException if the development namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> publishDesignDocument(final String name) {
    return publishDesignDocument(name, publishDesignDocumentOptions());
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException if the development namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> publishDesignDocument(final String name, final PublishDesignDocumentOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    notNull(options, "PublishDesignDocumentOptions", () -> new ReducedViewErrorContext(name, null, bucket));

    return coreManager.publishDesignDocument(name, options.build());
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @param namespace namespace to remove it from
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> dropDesignDocument(final String name, final DesignDocumentNamespace namespace) {
    return dropDesignDocument(name, namespace, dropDesignDocumentOptions());
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @param namespace namespace to remove it from
   * @param options additional optional arguments (timeout, retry, etc.)
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> dropDesignDocument(final String name, final DesignDocumentNamespace namespace,
                                                    final DropDesignDocumentOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    notNull(namespace, "DesignDocumentNamespace", () -> new ReducedViewErrorContext(name, null, bucket));
    notNull(options, "DropDesignDocumentOptions", () -> new ReducedViewErrorContext(name, null, bucket));

    DropDesignDocumentOptions.Built bltOptions = options.build();
    return coreManager.dropDesignDocument(name, namespace == PRODUCTION, bltOptions)
      .exceptionally(t -> {
        if (bltOptions.ignoreIfNotExists() && hasCause(t, DesignDocumentNotFoundException.class)) {
          return null;
        }
        throwIfUnchecked(t);
        throw new RuntimeException(t);
      });
  }

  private static ObjectNode toJson(final DesignDocument doc) {
    final ObjectNode root = Mapper.createObjectNode();
    final ObjectNode views = root.putObject("views");
    doc.views().forEach((k, v) -> {
      ObjectNode viewNode = Mapper.createObjectNode();
      viewNode.put("map", v.map());
      v.reduce().ifPresent(r -> viewNode.put("reduce", r));
      views.set(k, viewNode);
    });
    return root;
  }
}
