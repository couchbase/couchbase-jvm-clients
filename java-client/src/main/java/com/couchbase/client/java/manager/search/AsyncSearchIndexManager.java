/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.search.GenericSearchRequest;
import com.couchbase.client.core.msg.search.GenericSearchResponse;
import com.couchbase.client.core.msg.view.GenericViewRequest;
import com.couchbase.client.core.msg.view.GenericViewResponse;
import com.couchbase.client.java.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.manager.search.AllowQueryingSearchIndexOptions.allowQueryingSearchIndexOptions;
import static com.couchbase.client.java.manager.search.AnalyzeDocumentOptions.analyzeDocumentOptions;
import static com.couchbase.client.java.manager.search.DisallowQueryingSearchIndexOptions.disallowQueryingSearchIndexOptions;
import static com.couchbase.client.java.manager.search.DropSearchIndexOptions.dropSearchIndexOptions;
import static com.couchbase.client.java.manager.search.FreezePlanSearchIndexOptions.freezePlanSearchIndexOptions;
import static com.couchbase.client.java.manager.search.GetAllSearchIndexesOptions.getAllSearchIndexesOptions;
import static com.couchbase.client.java.manager.search.GetIndexedSearchIndexOptions.getIndexedSearchIndexOptions;
import static com.couchbase.client.java.manager.search.GetSearchIndexOptions.getSearchIndexOptions;
import static com.couchbase.client.java.manager.search.PauseIngestSearchIndexOptions.pauseIngestSearchIndexOptions;
import static com.couchbase.client.java.manager.search.ResumeIngestSearchIndexOptions.resumeIngestSearchIndexOptions;
import static com.couchbase.client.java.manager.search.UnfreezePlanSearchIndexOptions.unfreezePlanSearchIndexOptions;
import static com.couchbase.client.java.manager.search.UpsertSearchIndexOptions.upsertSearchIndexOptions;

/**
 * The {@link AsyncSearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class AsyncSearchIndexManager {

  private final Core core;
  private final CoreEnvironment environment;

  public AsyncSearchIndexManager(final Core core) {
    this.core = core;
    this.environment = core.context().environment();
  }

  private static String indexesPath() {
    return "/api/index";
  }

  private static String indexPath(final String indexName) {
    return indexesPath() + "/" + urlEncode(indexName);
  }

  private static String indexCountPath(final String indexName) {
    return indexPath(indexName) + "/count";
  }

  private static String analyzeDocumentPath(final String indexName) {
    return indexPath(indexName) + "/analyzeDoc";
  }

  private static String pauseIngestPath(final String indexName) {
    return indexPath(indexName) + "/ingestControl/pause";
  }

  private static String resumeIngestPath(final String indexName) {
    return indexPath(indexName) + "/ingestControl/resume";
  }

  private static String allowQueryingPath(final String indexName) {
    return indexPath(indexName) + "/queryControl/allow";
  }

  private static String disallowQueryingPath(final String indexName) {
    return indexPath(indexName) + "/queryControl/disallow";
  }

  private static String freezePlanPath(final String indexName) {
    return indexPath(indexName) + "/planFreezeControl/freeze";
  }

  private static String unfreezePlanPath(final String indexName) {
    return indexPath(indexName) + "/planFreezeControl/unfreeze";
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(final String name) {
    return getIndex(name, getSearchIndexOptions());
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(final String name, GetSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    GetSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_GET_INDEX,
      built.parentSpan().orElse(null)
    );

    return getAllIndexes(getAllSearchIndexesOptions().parentSpan(span)).thenApply(indexes -> {
      Optional<SearchIndex> found = indexes.stream().filter(i -> i.name().equals(name)).findFirst();
      if (found.isPresent()) {
        return found.get();
      }
      throw new IndexNotFoundException(name);
    }).whenComplete((r, t) -> {
      if (span != null) {
        span.end();
      }
    });
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public CompletableFuture<List<SearchIndex>> getAllIndexes() {
    return getAllIndexes(getAllSearchIndexesOptions());
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public CompletableFuture<List<SearchIndex>> getAllIndexes(final GetAllSearchIndexesOptions options) {
    GetAllSearchIndexesOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_GET_ALL_INDEXES,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.GET, indexesPath(), span).thenApply(response -> {
      JsonNode rootNode = Mapper.decodeIntoTree(response.content());
      JsonNode indexDefs = rootNode.get("indexDefs").get("indexDefs");
      Map<String, SearchIndex> indexes = Mapper.convertValue(
        indexDefs,
        new TypeReference<Map<String, SearchIndex>>() {}
      );
      return new ArrayList<>(indexes.values());
    });
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(final String name) {
    return getIndexedDocumentsCount(name, getIndexedSearchIndexOptions());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(final String name, final GetIndexedSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    GetIndexedSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_GET_IDX_DOC_COUNT,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.GET, indexCountPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to get indexed documents count search index", throwable);
      })
      .thenApply(response -> {
        JsonNode rootNode = Mapper.decodeIntoTree(response.content());
        return rootNode.get("count").asLong();
      });
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(final SearchIndex index) {
    return upsertIndex(index, upsertSearchIndexOptions());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(final SearchIndex index, final UpsertSearchIndexOptions options) {
    notNull(index, "Search Index");

    UpsertSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_UPSERT_INDEX,
      built.parentSpan().orElse(null)
    );

    return sendRequest(() -> {
      ByteBuf payload = Unpooled.wrappedBuffer(index.toJson().getBytes(StandardCharsets.UTF_8));
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.PUT,
        indexPath(index.name()),
        payload
      );
      request.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
      request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
      request.headers().set(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes());
      return request;
    }, false, span).thenApply(response -> null);
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(final String name) {
    return dropIndex(name, dropSearchIndexOptions());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(final String name, final DropSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    DropSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_DROP_INDEX,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.DELETE, indexPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to drop search index", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(final String name, final JsonObject document) {
    return analyzeDocument(name, document, analyzeDocumentOptions());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(final String name, final JsonObject document,
                                                             final AnalyzeDocumentOptions options) {
    notNullOrEmpty(name, "Search Index Name");
    notNull(document, "Document");

    AnalyzeDocumentOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_ANALYZE_DOCUMENT,
      built.parentSpan().orElse(null)
    );

    return sendRequest(() -> {
      ByteBuf content = Unpooled.wrappedBuffer(Mapper.encodeAsBytes(document.toMap()));
      FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        analyzeDocumentPath(name),
        content
      );
      request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
      request.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
      return request;
    }, true, span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("Page not found")) {
          throw new FeatureNotAvailableException("Document analysis is not available on this server version!");
        } else if (throwable.getMessage().contains("no indexName:")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to analyze search document", throwable);
      })
      .thenApply(response -> {
        JsonNode rootNode = Mapper.decodeIntoTree(response.content());
        List<Map<String, Object>> analyzed = Mapper.convertValue(
          rootNode.get("analyzed"),
          new TypeReference<List<Map<String, Object>>>() {}
        );
        return analyzed.stream().filter(Objects::nonNull).map(JsonObject::from).collect(Collectors.toList());
      });
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(final String name) {
    return pauseIngest(name, pauseIngestSearchIndexOptions());
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(final String name, final PauseIngestSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    PauseIngestSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_PAUSE_INGEST,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, pauseIngestPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to pause search index ingest", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(final String name) {
    return resumeIngest(name, resumeIngestSearchIndexOptions());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(final String name, final ResumeIngestSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    ResumeIngestSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_RESUME_INGEST,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, resumeIngestPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to resume search index ingest", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(final String name) {
    return allowQuerying(name, allowQueryingSearchIndexOptions());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(final String name, final AllowQueryingSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    AllowQueryingSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_ALLOW_QUERYING,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, allowQueryingPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to allow querying on the search index", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(final String name) {
    return disallowQuerying(name, disallowQueryingSearchIndexOptions());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(final String name, final DisallowQueryingSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    DisallowQueryingSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_DISALLOW_QUERYING,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, disallowQueryingPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to disallow querying on the search index", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(final String name) {
    return freezePlan(name, freezePlanSearchIndexOptions());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(final String name, final FreezePlanSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    FreezePlanSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_FREEZE_PLAN,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, freezePlanPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to freeze plan on the search index", throwable);
      })
      .thenApply(response -> null);
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(final String name) {
    return unfreezePlan(name, unfreezePlanSearchIndexOptions());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(final String name, final UnfreezePlanSearchIndexOptions options) {
    notNullOrEmpty(name, "Search Index Name");

    UnfreezePlanSearchIndexOptions.Built built = options.build();
    RequestSpan span = buildSpan(
      TracingIdentifiers.SPAN_REQUEST_MS_UNFREEZE_PLAN,
      built.parentSpan().orElse(null)
    );

    return sendRequest(HttpMethod.POST, unfreezePlanPath(name), span)
      .exceptionally(throwable -> {
        if (throwable.getMessage().contains("index not found")) {
          throw new IndexNotFoundException(name);
        }
        throw new CouchbaseException("Failed to unfreeze plan on the search index", throwable);
      })
      .thenApply(response -> null);
  }

  private CompletableFuture<GenericSearchResponse> sendRequest(final HttpMethod method, final String path,
                                                               final RequestSpan span) {
    return sendRequest(
      () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path),
      method == HttpMethod.GET,
      span
    );
  }

  private CompletableFuture<GenericSearchResponse> sendRequest(final Supplier<FullHttpRequest> httpRequest,
                                                               boolean idempotent, RequestSpan span) {
    GenericSearchRequest request = new GenericSearchRequest(
      environment.timeoutConfig().managementTimeout(),
      core.context(),
      environment.retryStrategy(),
      httpRequest,
      idempotent,
      span
    );

    return sendRequest(request).whenComplete((r, t) -> {
      if (span != null) {
        span.end();
      }
    });
  }

  private CompletableFuture<GenericSearchResponse> sendRequest(final GenericSearchRequest request) {
    core.send(request);
    return request.response();
  }

  private RequestSpan buildSpan(final String spanName, final RequestSpan parent) {
    return core.context().environment().requestTracer().requestSpan(spanName, parent);
  }

}
