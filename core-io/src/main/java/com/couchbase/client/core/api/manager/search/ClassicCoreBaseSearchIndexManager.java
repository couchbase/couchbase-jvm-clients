/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.manager.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.util.SearchCapabilityCheck;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.util.concurrent.CompleteFuture;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

@Stability.Internal
public abstract class ClassicCoreBaseSearchIndexManager implements CoreSearchIndexManager {

  protected final Core core;
  private final CoreHttpClient searchHttpClient;

  @Stability.Internal
  public ClassicCoreBaseSearchIndexManager(Core core) {
    this.core = core;
    this.searchHttpClient = core.httpClient(RequestTarget.search());
  }

  protected static String globalIndexesPath() {
    return "/api/index";
  }

  protected static String globalIndexPath(String indexName) {
    return globalIndexesPath() + "/" + urlEncode(indexName);
  }

  abstract String indexesPath();

  String indexPath(String indexName) {
    return indexesPath() + "/" + urlEncode(indexName);
  }

  String indexCountPath(String indexName) {
    return indexPath(indexName) + "/count";
  }

  String analyzeDocumentPath(String indexName) {
    return indexPath(indexName) + "/analyzeDoc";
  }

  String pauseIngestPath(String indexName) {
    return indexPath(indexName) + "/ingestControl/pause";
  }

  String resumeIngestPath(String indexName) {
    return indexPath(indexName) + "/ingestControl/resume";
  }

  String allowQueryingPath(String indexName) {
    return indexPath(indexName) + "/queryControl/allow";
  }

  String disallowQueryingPath(String indexName) {
    return indexPath(indexName) + "/queryControl/disallow";
  }

  String freezePlanPath(String indexName) {
    return indexPath(indexName) + "/planFreezeControl/freeze";
  }

  String unfreezePlanPath(String indexName) {
    return indexPath(indexName) + "/planFreezeControl/unfreeze";
  }

  // Allows the implementation to perform any initial checks it needs to - for example, that the cluster supports
  // this operation.
  abstract CompletableFuture<Void> initialCheck(Duration timeout);

  public CompletableFuture<CoreSearchIndex> getIndex(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);

    RequestSpan span = CbTracing.newSpan(
            core.context(),
            TracingIdentifiers.SPAN_REQUEST_MS_GET_INDEX,
            options.parentSpan().orElse(null)
    );

    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> getAllIndexes(CoreCommonOptions.of(timeout, null, span)))
            .thenApply(indexes -> indexes.stream()
                    .filter(i -> i.name().equals(name))
                    .findFirst().orElseThrow(() -> new IndexNotFoundException(name)))
            .whenComplete((r, t) -> span.end());
  }

  public CompletableFuture<List<CoreSearchIndex>> getAllIndexes(CoreCommonOptions options) {
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());
    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.get(path(indexesPath()), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_GET_ALL_INDEXES)
                    .exec(core)
                    .thenApply(response -> {
                      JsonNode rootNode = Mapper.decodeIntoTree(response.content());
                      JsonNode indexDefs = rootNode.get("indexDefs").get("indexDefs");
                      Map<String, CoreSearchIndex> indexes = Mapper.convertValue(
                              indexDefs,
                              new TypeReference<Map<String, CoreSearchIndex>>() {
                              }
                      );
                      return indexes == null ? Collections.emptyList() : new ArrayList<>(indexes.values());
                    }));
  }

  public CompletableFuture<Long> getIndexedDocumentsCount(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.get(path(indexCountPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_GET_IDX_DOC_COUNT)
                    .exec(core)
                    .thenApply(response -> {
                      JsonNode rootNode = Mapper.decodeIntoTree(response.content());
                      return rootNode.get("count").asLong();
                    }));
  }

  public CompletableFuture<Void> upsertIndex(CoreSearchIndex index, CoreCommonOptions options) {
    notNull(index, "Search Index");
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> vectorIndexCheck(core, index, timeout))
            .thenCompose(ignore -> searchHttpClient.put(path(indexPath(index.name())), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_UPSERT_INDEX)
                    .json(index.toJson().getBytes(UTF_8))
                    .header(HttpHeaderNames.CACHE_CONTROL, "no-cache")
                    .exec(core)
                    .thenApply(response -> null));
  }

  private static CompletableFuture<Void> vectorIndexCheck(Core core, CoreSearchIndex index, Duration timeout) {
    if (index.containsVectorMappings()) {
      return SearchCapabilityCheck.vectorSearchCapabilityCheck(core, timeout);
    }
    return CompletableFuture.completedFuture(null);
  }

  public CompletableFuture<Void> dropIndex(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.delete(path(indexPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_DROP_INDEX)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<List<ObjectNode>> analyzeDocument(String name, ObjectNode document,
                                                             CoreCommonOptions options) {
    validateSearchParams(name, options);
    notNull(document, "Document");
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(analyzeDocumentPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_ANALYZE_DOCUMENT)
                    .json(Mapper.encodeAsBytes(document))
                    .exec(core)
                    .exceptionally(throwable -> {
                      if (throwable.getMessage().contains("Page not found")) {
                        throw new FeatureNotAvailableException("Document analysis is not available on this server version!");
                      } else if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                      } else {
                        throw new CouchbaseException("Failed to analyze search document", throwable);
                      }
                    })
                    .thenApply(response -> {
                      JsonNode rootNode = Mapper.decodeIntoTree(response.content());
                      return Mapper.convertValue(
                              rootNode.get("analyzed"),
                              new TypeReference<List<ObjectNode>>() {
                              }
                      );
                    }));
  }

  public CompletableFuture<Void> pauseIngest(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(pauseIngestPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_PAUSE_INGEST)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<Void> resumeIngest(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(resumeIngestPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_RESUME_INGEST)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<Void> allowQuerying(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(allowQueryingPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_ALLOW_QUERYING)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<Void> disallowQuerying(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(disallowQueryingPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_DISALLOW_QUERYING)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<Void> freezePlan(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(freezePlanPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_FREEZE_PLAN)
                    .exec(core)
                    .thenApply(response -> null));
  }

  public CompletableFuture<Void> unfreezePlan(String name, CoreCommonOptions options) {
    validateSearchParams(name, options);
    Duration timeout = options.timeout().orElse(core.environment().timeoutConfig().managementTimeout());

    return initialCheck(timeout)
            .thenCompose(ignore -> searchHttpClient.post(path(unfreezePlanPath(name)), options)
                    .trace(TracingIdentifiers.SPAN_REQUEST_MS_UNFREEZE_PLAN)
                    .exec(core)
                    .thenApply(response -> null));
  }

  private void validateSearchParams(String name, CoreCommonOptions options) {
    notNullOrEmpty(name, "Search Index Name");
    notNull(options, "Search Index Options");
  }
}
