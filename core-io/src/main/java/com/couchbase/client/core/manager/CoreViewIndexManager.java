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

package com.couchbase.client.core.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
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
import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedViewErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerResponse;
import com.couchbase.client.core.msg.view.GenericViewRequest;
import com.couchbase.client.core.msg.view.GenericViewResponse;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.DELETE;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.PUT;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.removeStart;
import static com.couchbase.client.core.util.CbThrowables.findCause;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreViewIndexManager {
  private static final String DEV_PREFIX = "dev_";

  public static String requireUnqualifiedName(final String name) {
    if (name.startsWith(DEV_PREFIX)) {
      throw InvalidArgumentException.fromMessage(
          "Design document name '" + redactMeta(name) + "' must not start with '" + DEV_PREFIX + "'" +
              "; instead specify the " + namespaceToString(false) + " namespace when referring to the document.");
    }
    return name;
  }

  private class ConfigManager extends AbstractManagerSupport {
    public ConfigManager() {
      super(CoreViewIndexManager.this.core);
    }

    @Override
    public CompletableFuture<GenericManagerResponse> sendRequest(
        HttpMethod method, String path, Duration timeout, RetryStrategy retry, RequestSpan span) {
      return super.sendRequest(method, path, resolve(timeout), resolve(retry), span);
    }
  }

  protected final Core core;
  private final String bucket;

  public CoreViewIndexManager(Core core, String bucket) {
    this.core = requireNonNull(core);
    this.bucket = requireNonNull(bucket);
  }

  private static String adjustName(String name, boolean production) {
    if (production) {
      return removeStart(name, DEV_PREFIX);
    }
    return name.startsWith(DEV_PREFIX) ? name : DEV_PREFIX + name;
  }

  private String pathForDesignDocument(String name, boolean production) {
    name = adjustName(name, production);
    return "/" + urlEncode(bucket) + "/_design/" + urlEncode(name);
  }

  private String pathForAllDesignDocuments() {
    return "/pools/default/buckets/" + urlEncode(bucket) + "/ddocs";
  }

  /**
   * Returns map of design doc name to JSON.
   * <p>
   * JSON structure is same as returned by {@link #getDesignDocument}.
   */
  public CompletableFuture<Map<String, ObjectNode>> getAllDesignDocuments(boolean production,
                                                                          Duration timeout, RetryStrategy retry, RequestSpan parentSpan) {
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_GET_ALL_DD, parentSpan);
    span.setAttribute(TracingIdentifiers.ATTR_NAME, bucket);

    return new ConfigManager().sendRequest(GET, pathForAllDesignDocuments(), resolve(timeout), resolve(retry), span).thenApply(response -> {
      // Unlike the other view management requests, this request goes through the config manager endpoint.
      // That endpoint treats any complete HTTP response as a success, so it's up to us to check the status code.
      if (response.status() != ResponseStatus.SUCCESS) {
        throw new CouchbaseException(
            "Failed to get all design documents" +
                "; response status=" + response.status() +
                "; response body=" + new String(response.content(), UTF_8));
      }
      return parseAllDesignDocuments(Mapper.decodeIntoTree(response.content()), production);
    });
  }

  private static Map<String, ObjectNode> parseAllDesignDocuments(JsonNode node, boolean production) {
    Map<String, ObjectNode> result = new LinkedHashMap<>();
    node.get("rows").forEach(row -> {
      String metaId = row.path("doc").path("meta").path("id").asText();
      String ddocName = removeStart(metaId, "_design/");
      if (namespaceContainsName(ddocName, production)) {
        JsonNode ddocDef = row.path("doc").path("json");
        result.put(ddocName, (ObjectNode) ddocDef);
      }
    });
    return result;
  }

  private static boolean namespaceContainsName(final String name, final boolean production) {
    return (production && !name.startsWith(DEV_PREFIX))
        || (!production && name.startsWith(DEV_PREFIX));
  }

  /**
   * Returns the named design document from the specified namespace.
   *
   * @param name name of the design document to retrieve
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<byte[]> getDesignDocument(String name, boolean production,
                                                     Duration timeout, RetryStrategy retry, RequestSpan parentSpan) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));

    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_GET_DD, parentSpan);

    return sendRequest(GET, pathForDesignDocument(name, production), resolve(timeout), resolve(retry), span)
        .exceptionally(t -> {
          String namespace = namespaceToString(production);
          throw notFound(t)
              ? DesignDocumentNotFoundException.forName(name, namespace)
              : new CouchbaseException("Failed to get design document [" + redactMeta(name) + "] from namespace " + namespace, t);
        })
        .thenApply(GenericViewResponse::content);
  }

  private static String namespaceToString(boolean production) {
    return production ? "PRODUCTION" : "DEVELOPMENT";
  }

  /**
   * Stores the design document on the server under the specified namespace, replacing any existing document
   * with the same name.
   *
   * @param doc document to store
   */
  public CompletableFuture<Void> upsertDesignDocument(final String docName, final byte[] doc, final boolean production,
                                                      final Duration timeout, final RetryStrategy retry, final RequestSpan parentSpan) {
    notNull(doc, "DesignDocument", () -> new ReducedViewErrorContext(null, null, bucket));

    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_UPSERT_DD, parentSpan);

    return sendJsonRequest(PUT, pathForDesignDocument(docName, production), doc, resolve(timeout), resolve(retry), span)
        .thenApply(response -> null);
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @throws DesignDocumentNotFoundException if the development namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> publishDesignDocument(final String name,
                                                       final Duration timeout, final RetryStrategy retry, final RequestSpan parentSpan) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_PUBLISH_DD, parentSpan);

    return getDesignDocument(name, false, resolve(timeout), resolve(retry), span)
        .thenCompose(doc -> upsertDesignDocument(name, doc, true, resolve(timeout), resolve(retry), span))
        .whenComplete((r, t) -> span.end());
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> dropDesignDocument(final String name, final boolean production,
                                                    final Duration timeout, final RetryStrategy retry, final RequestSpan parentSpan) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_DROP_DD, parentSpan);

    return sendRequest(DELETE, pathForDesignDocument(name, production), resolve(timeout), resolve(retry), span)
        .exceptionally(t -> {
          String namespace = namespaceToString(production);
          if (notFound(t)) {
            throw DesignDocumentNotFoundException.forName(name, namespace);
          } else {
            throw new CouchbaseException(
                "Failed to drop design document [" + redactMeta(name) + "] from namespace " + namespace,
                t
            );
          }
        })
        .thenApply(response -> null);
  }

  private static boolean notFound(final Throwable t) {
    return getHttpStatusCode(t) == HttpResponseStatus.NOT_FOUND.code();
  }

  private static int getHttpStatusCode(final Throwable t) {
    return findCause(t, HttpStatusCodeException.class)
        .map(HttpStatusCodeException::code)
        .orElse(0);
  }

  private CompletableFuture<GenericViewResponse> sendRequest(final GenericViewRequest request) {
    core.send(request);
    return request.response();
  }

  private CompletableFuture<GenericViewResponse> sendRequest(final HttpMethod method, final String path,
                                                             final Duration timeout,
                                                             final RetryStrategy retry,
                                                             final RequestSpan span) {
    return sendRequest(new GenericViewRequest(timeout, core.context(), retry,
        () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == GET, bucket, span))
        .whenComplete((r, t) -> span.end());
  }

  private CompletableFuture<GenericViewResponse> sendJsonRequest(final HttpMethod method, final String path,
                                                                 final byte[] body,
                                                                 final Duration timeout,
                                                                 final RetryStrategy retry,
                                                                 final RequestSpan span) {
    return sendRequest(new GenericViewRequest(timeout, core.context(), retry, () -> {
      ByteBuf content = Unpooled.wrappedBuffer(body);
      DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content);
      req.headers().add("Content-Type", HttpHeaderValues.APPLICATION_JSON);
      req.headers().add("Content-Length", content.readableBytes());
      return req;
    }, method == GET, bucket, span))
        .whenComplete((r, t) -> span.end());
  }

  private RequestSpan buildSpan(final String spanName, final RequestSpan parent) {
    RequestSpan span = core.context().environment().requestTracer().requestSpan(spanName, parent);
    span.setAttribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    return span;
  }

  private Duration resolve(Duration timeout) {
    return timeout != null ? timeout : core.context().environment().timeoutConfig().managementTimeout();
  }

  private RetryStrategy resolve(RetryStrategy retry) {
    return retry != null ? retry : core.context().environment().retryStrategy();
  }
}
