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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.msg.view.GenericViewRequest;
import com.couchbase.client.core.msg.view.GenericViewResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.manager.ManagerSupport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.DELETE;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.PUT;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.removeStart;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.java.manager.view.DesignDocumentNamespace.DEVELOPMENT;
import static com.couchbase.client.java.manager.view.DesignDocumentNamespace.PRODUCTION;
import static com.couchbase.client.java.manager.view.DesignDocumentNamespace.requireUnqualified;
import static com.couchbase.client.java.manager.view.DropDesignDocumentOptions.dropDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.GetAllDesignDocumentsOptions.getAllDesignDocumentsOptions;
import static com.couchbase.client.java.manager.view.GetDesignDocumentOptions.getDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.PublishDesignDocumentOptions.publishDesignDocumentOptions;
import static com.couchbase.client.java.manager.view.UpsertDesignDocumentOptions.upsertDesignDocumentOptions;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class AsyncViewIndexManager {

  private final Core core;
  private final String bucket;

  private class ConfigManager extends ManagerSupport {
    public ConfigManager() {
      super(core);
    }

    public CompletableFuture<GenericManagerResponse> sendRequest(HttpMethod method, String path) {
      return super.sendRequest(method, path);
    }
  }

  public AsyncViewIndexManager(Core core, String bucket) {
    this.core = requireNonNull(core);
    this.bucket = requireNonNull(bucket);
  }

  private String pathForDesignDocument(String name, DesignDocumentNamespace namespace) {
    name = namespace.adjustName(requireUnqualified(name));
    return "/" + urlEncode(bucket) + "/_design/" + urlEncode(name);
  }

  private String pathForAllDesignDocuments() {
    return "/pools/default/buckets/" + urlEncode(bucket) + "/ddocs";
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   */
  public CompletableFuture<List<DesignDocument>> getAllDesignDocuments(DesignDocumentNamespace namespace) {
    return getAllDesignDocuments(namespace, getAllDesignDocumentsOptions());
  }

  /**
   * Returns all of the design documents in the specified namespace.
   *
   * @param namespace namespace to query
   * @param options additional optional arguments (timeout, retry, etc.)
   */
  public CompletableFuture<List<DesignDocument>> getAllDesignDocuments(DesignDocumentNamespace namespace, GetAllDesignDocumentsOptions options) {
    return new ConfigManager().sendRequest(GET, pathForAllDesignDocuments()).thenApply(response -> {
      // Unlike the other view management requests, this request goes through the config manager endpoint.
      // That endpoint treats any complete HTTP response as a success, so it's up to us to check the status code.
      if (response.status() != ResponseStatus.SUCCESS) {
        throw new CouchbaseException("Failed to get all design documents; response status=" + response.status() + "; response body=" + new String(response.content(), UTF_8));
      }
      return parseAllDesignDocuments(Mapper.decodeIntoTree(response.content()), namespace);
    });
  }

  private static List<DesignDocument> parseAllDesignDocuments(JsonNode node, DesignDocumentNamespace namespace) {
    List<DesignDocument> result = new ArrayList<>();
    node.get("rows").forEach(row -> {
      String metaId = row.path("doc").path("meta").path("id").asText();
      String ddocName = removeStart(metaId, "_design/");
      if (namespace.contains(ddocName)) {
        JsonNode ddocDef = row.path("doc").path("json");
        result.add(parseDesignDocument(ddocName, ddocDef));
      }
    });
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
    return sendRequest(GET, pathForDesignDocument(name, namespace), options.build())
        .exceptionally(t -> {
          throw notFound(t)
              ? DesignDocumentNotFoundException.forName(name, namespace)
              : new CouchbaseException("Failed to get design document [" + redactMeta(name) + "] from namespace " + namespace, t);
        })
        .thenApply(response ->
            parseDesignDocument(name, Mapper.decodeIntoTree(response.content())));
  }

  private static DesignDocument parseDesignDocument(String name, JsonNode node) {
    ObjectNode viewsNode = (ObjectNode) node.path("views");
    Map<String, View> views = Mapper.convertValue(viewsNode, new TypeReference<Map<String, View>>() {
    });
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
  public CompletableFuture<Void> upsertDesignDocument(DesignDocument doc, DesignDocumentNamespace namespace, UpsertDesignDocumentOptions options) {
    final ObjectNode body = toJson(doc);
    return sendJsonRequest(PUT, pathForDesignDocument(doc.name(), namespace), options.build(), body)
        .thenApply(response -> null);
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @throws DesignDocumentNotFoundException if the development namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> publishDesignDocument(String name) {
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
  public CompletableFuture<Void> publishDesignDocument(String name, PublishDesignDocumentOptions options) {
    return getDesignDocument(name, DEVELOPMENT)
        .thenCompose(doc -> upsertDesignDocument(doc, PRODUCTION));
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @param namespace namespace to remove it from
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> dropDesignDocument(String name, DesignDocumentNamespace namespace) {
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
  public CompletableFuture<Void> dropDesignDocument(String name, DesignDocumentNamespace namespace, DropDesignDocumentOptions options) {
    return sendRequest(DELETE, pathForDesignDocument(name, namespace), options.build())
        .exceptionally(t -> {
          throw notFound(t)
              ? DesignDocumentNotFoundException.forName(name, namespace)
              : new CouchbaseException("Failed to drop design document [" + redactMeta(name) + "] from namespace " + namespace, t);
        })
        .thenApply(response -> null);
  }

  private static boolean notFound(Throwable t) {
    return getHttpStatusCode(t) == HttpResponseStatus.NOT_FOUND.code();
  }

  private static int getHttpStatusCode(Throwable t) {
    return findCause(t, HttpStatusCodeException.class)
        .map(HttpStatusCodeException::code)
        .orElse(0);
  }

  private static ObjectNode toJson(DesignDocument doc) {
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

  private CompletableFuture<GenericViewResponse> sendRequest(GenericViewRequest request) {
    core.send(request);
    return request.response();
  }

  private CompletableFuture<GenericViewResponse> sendRequest(HttpMethod method, String path, CommonOptions<?>.BuiltCommonOptions options) {
    return sendRequest(new GenericViewRequest(timeout(options), core.context(), retryStrategy(options),
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == GET, bucket));
  }

  private CompletableFuture<GenericViewResponse> sendJsonRequest(HttpMethod method, String path, CommonOptions<?>.BuiltCommonOptions options, Object body) {
    return sendRequest(new GenericViewRequest(timeout(options), core.context(), retryStrategy(options), () -> {
      ByteBuf content = Unpooled.copiedBuffer(Mapper.encodeAsBytes(body));
      DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content);
      req.headers().add("Content-Type", HttpHeaderValues.APPLICATION_JSON);
      req.headers().add("Content-Length", content.readableBytes());
      return req;
    }, method == GET, bucket));
  }

  private Duration timeout(CommonOptions<?>.BuiltCommonOptions options) {
    // Even though most of the requests are dispatched to the view service,
    // these are management operations so use the manager timeout.
    return options.timeout().orElse(core.context().environment().timeoutConfig().managementTimeout());
  }

  private RetryStrategy retryStrategy(CommonOptions<?>.BuiltCommonOptions options) {
    return options.retryStrategy().orElse(core.context().environment().retryStrategy());
  }
}
