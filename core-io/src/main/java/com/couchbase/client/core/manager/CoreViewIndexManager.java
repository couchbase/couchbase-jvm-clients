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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DesignDocumentNotFoundException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedViewErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbCollections.mapOf;
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

  public static String requireUnqualifiedName(String name) {
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
  }

  protected final Core core;
  private final String bucket;
  protected final CoreHttpClient viewService;

  public CoreViewIndexManager(Core core, String bucket) {
    this.core = requireNonNull(core);
    this.bucket = requireNonNull(bucket);
    this.viewService = core.httpClient(RequestTarget.views(bucket));
  }

  private static String adjustName(String name, boolean production) {
    if (production) {
      return removeStart(name, DEV_PREFIX);
    }
    return name.startsWith(DEV_PREFIX) ? name : DEV_PREFIX + name;
  }

  private CoreHttpPath pathForDesignDocument(String name, boolean production) {
    return path("/{bucket}/_design/{ddoc}", mapOf(
        "bucket", bucket,
        "ddoc", adjustName(name, production)));
  }

  private String pathForAllDesignDocuments() {
    return "/pools/default/buckets/" + urlEncode(bucket) + "/ddocs";
  }

  /**
   * Returns map of design doc name to JSON.
   * <p>
   * JSON structure is same as returned by {@link #getDesignDocument}.
   */
  public CompletableFuture<Map<String, ObjectNode>> getAllDesignDocuments(boolean production, CoreCommonOptions options) {
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_GET_ALL_DD, options.parentSpan());
    span.attribute(TracingIdentifiers.ATTR_NAME, bucket);

    Duration timeout = options.timeout().orElse(core.context().environment().timeoutConfig().managementTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(null);
    return new ConfigManager().sendRequest(GET, pathForAllDesignDocuments(), timeout, retryStrategy, span).thenApply(response -> {
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

  private static boolean namespaceContainsName(String name, boolean production) {
    return (production && !name.startsWith(DEV_PREFIX))
        || (!production && name.startsWith(DEV_PREFIX));
  }

  /**
   * Returns the named design document from the specified namespace.
   *
   * @param name name of the design document to retrieve
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<byte[]> getDesignDocument(String name, boolean production, CoreCommonOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));

    return viewService.get(pathForDesignDocument(name, production), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MV_GET_DD)
        .exec(core)
        .exceptionally(t -> {
          String namespace = namespaceToString(production);
          throw notFound(t)
              ? DesignDocumentNotFoundException.forName(name, namespace)
              : new CouchbaseException("Failed to get design document [" + redactMeta(name) + "] from namespace " + namespace, t);
        })
        .thenApply(CoreHttpResponse::content);
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
  public CompletableFuture<Void> upsertDesignDocument(String docName, byte[] doc, boolean production, CoreCommonOptions options) {
    notNull(doc, "DesignDocument", () -> new ReducedViewErrorContext(null, null, bucket));

    return viewService.put(pathForDesignDocument(docName, production), options)
        .json(doc)
        .trace(TracingIdentifiers.SPAN_REQUEST_MV_UPSERT_DD)
        .exec(core)
        .thenApply(response -> null);
  }

  /**
   * Convenience method that gets a the document from the development namespace
   * and upserts it to the production namespace.
   *
   * @param name name of the development design document
   * @throws DesignDocumentNotFoundException if the development namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> publishDesignDocument(String name, CoreCommonOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MV_PUBLISH_DD, options.parentSpan());

    CoreCommonOptions childOptions = options.withParentSpan(span);
    return getDesignDocument(name, false, childOptions)
        .thenCompose(doc -> upsertDesignDocument(name, doc, true, childOptions))
        .whenComplete((r, t) -> span.end());
  }

  /**
   * Removes a design document from the server.
   *
   * @param name name of the document to remove
   * @throws DesignDocumentNotFoundException if the namespace does not contain a document with the given name
   */
  public CompletableFuture<Void> dropDesignDocument(String name, boolean production, CoreCommonOptions options) {
    notNullOrEmpty(name, "Name", () -> new ReducedViewErrorContext(null, null, bucket));

    return viewService.delete(pathForDesignDocument(name, production), options)
        .trace(TracingIdentifiers.SPAN_REQUEST_MV_DROP_DD)
        .exec(core)
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

  private static boolean notFound(Throwable t) {
    return getHttpStatusCode(t) == HttpResponseStatus.NOT_FOUND.code();
  }

  private static int getHttpStatusCode(Throwable t) {
    return findCause(t, HttpStatusCodeException.class)
        .map(HttpStatusCodeException::code)
        .orElse(0);
  }

  private RequestSpan buildSpan(String spanName, Optional<RequestSpan> parent) {
    return CbTracing.newSpan(core.context(), spanName, parent.orElse(null));
  }
}
