/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.search.facet.CoreSearchFacet;
import com.couchbase.client.core.api.search.queries.CoreSearchRequest;
import com.couchbase.client.core.api.search.result.CoreDateRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreNumericRangeSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreReactiveSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchFacetResult;
import com.couchbase.client.core.api.search.result.CoreSearchMetrics;
import com.couchbase.client.core.api.search.result.CoreSearchResult;
import com.couchbase.client.core.api.search.result.CoreSearchRow;
import com.couchbase.client.core.api.search.result.CoreSearchStatus;
import com.couchbase.client.core.api.search.result.CoreTermSearchFacetResult;
import com.couchbase.client.core.api.search.util.SearchCapabilityCheck;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.context.ReducedSearchErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.msg.search.SearchResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClassicCoreSearchOps implements CoreSearchOps {
  private static final byte[] NULL = new byte[]{'n', 'u', 'l', 'l'};

  private final Core core;

  /**
   * Determines whether this is being used for global search indexes (accessed from the Cluster level), or scoped
   * search indexes (accessed from the Scope level).
   */
  private final @Nullable CoreBucketAndScope scope;

  public ClassicCoreSearchOps(Core core, @Nullable CoreBucketAndScope scope) {
    this.core = core;
    this.scope = scope;
  }

  CoreAsyncResponse<Void> preflightCheckScopedIndexes(Duration timeout) {
    if (scope != null) {
      return new CoreAsyncResponse<>(SearchCapabilityCheck.scopedSearchIndexCapabilityCheck(core, timeout), () -> {
      });
    }
    return new CoreAsyncResponse<>(CompletableFuture.completedFuture(null), () -> {
    });
  }

  CoreAsyncResponse<Void> preflightCheckVectorIndexes(boolean requiresVectorIndexSupport, Duration timeout) {
    if (requiresVectorIndexSupport) {
      return new CoreAsyncResponse<>(SearchCapabilityCheck.vectorSearchCapabilityCheck(core, timeout), () -> {
      });
    }
    return new CoreAsyncResponse<>(CompletableFuture.completedFuture(null), () -> {
    });
  }

  @Override
  public CoreAsyncResponse<CoreSearchResult> searchQueryAsync(String indexName, CoreSearchQuery query, CoreSearchOptions options) {
    options.validate();
    Duration timeout = options.commonOptions().timeout().orElse(core.environment().timeoutConfig().searchTimeout());
    return searchAsyncShared(searchRequest(indexName, query, options), false, timeout);
  }

  @Override
  public Mono<CoreReactiveSearchResult> searchQueryReactive(String indexName, CoreSearchQuery query, CoreSearchOptions options) {
    options.validate();
    Duration timeout = options.commonOptions().timeout().orElse(core.environment().timeoutConfig().searchTimeout());
    return searchReactiveShared(searchRequest(indexName, query, options), false, timeout);
  }

  private static Map<String, CoreSearchFacetResult> parseFacets(final SearchChunkTrailer trailer) {
    byte[] rawFacets = trailer.facets();
    if (rawFacets == null || rawFacets.length == 0 || Arrays.equals(rawFacets, NULL)) {
      return Collections.emptyMap();
    }

    ObjectNode objectNode = (ObjectNode) Mapper.decodeIntoTree(rawFacets);
    Map<String, CoreSearchFacetResult> facets = new HashMap<>();

    forEachField(objectNode, (facetName, facetEntry) -> {
      // Copy the facet name into the facet JSON node to simplify data binding
      ((ObjectNode) facetEntry).set("$name", new TextNode(facetName));

      if (facetEntry.has("numeric_ranges")) {
        facets.put(facetName, Mapper.convertValue(facetEntry, CoreNumericRangeSearchFacetResult.class));
      } else if (facetEntry.has("date_ranges")) {
        facets.put(facetName, Mapper.convertValue(facetEntry, CoreDateRangeSearchFacetResult.class));
      } else {
        facets.put(facetName, Mapper.convertValue(facetEntry, CoreTermSearchFacetResult.class));
      }
    });

    return facets;
  }

  private static void forEachField(ObjectNode node, BiConsumer<String, JsonNode> consumer) {
    requireNonNull(consumer);
    node.fields().forEachRemaining(entry -> consumer.accept(entry.getKey(), entry.getValue()));
  }

  private static CoreSearchMetaData parseMeta(SearchResponse response, SearchChunkTrailer trailer) {
    CoreSearchStatus status = Mapper.decodeInto(response.header().getStatus(), CoreSearchStatus.class);
    CoreSearchMetrics metrics = new CoreSearchMetrics(
            Duration.ofNanos(trailer.took()),
            trailer.totalRows(),
            trailer.maxScore(),
            status.successCount(),
            status.errorCount()
    );
    return new CoreSearchMetaData(status.errors() == null ? Collections.emptyMap() : status.errors(), metrics);
  }

  private CoreEnvironment environment() {
    return core.context().environment();
  }

  private CoreResources coreResources() {
    return core.context().coreResources();
  }

  private ServerSearchRequest searchRequest(String indexName, CoreSearchQuery query, CoreSearchOptions opts) {
    notNullOrEmpty(indexName, "IndexName", () -> new ReducedSearchErrorContext(indexName, query));
    Duration timeout = opts.commonOptions().timeout().orElse(environment().timeoutConfig().searchTimeout());

    ObjectNode params = query.export();
    ObjectNode toSend = Mapper.createObjectNode();
    toSend.set("query", params);
    injectOptions(indexName, toSend, timeout, opts, false);
    byte[] bytes = toSend.toString().getBytes(StandardCharsets.UTF_8);

    RetryStrategy retryStrategy = opts.commonOptions().retryStrategy().orElse(environment().retryStrategy());

    RequestSpan span = coreResources()
            .requestTracer()
            .requestSpan(TracingIdentifiers.SPAN_REQUEST_SEARCH, opts.commonOptions().parentSpan().orElse(null));
    ServerSearchRequest request = new ServerSearchRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), indexName, bytes, span, scope);
    request.context().clientContext(opts.commonOptions().clientContext());
    return request;
  }

  private static void inject(ObjectNode queryJson, String field, @Nullable CoreSearchKeyset keyset) {
    if (keyset != null) {
      ArrayNode array = Mapper.createArrayNode();
      keyset.keys().forEach(array::add);
      queryJson.set(field, array);
    }
  }

  @Stability.Internal
  public static void injectOptions(String indexName, ObjectNode queryJson, Duration timeout, CoreSearchOptions opts, boolean disableShowRequest) {
    if (opts.limit() != null && opts.limit() >= 0) {
      queryJson.put("size", opts.limit());
    }
    if (opts.skip() != null && opts.skip() >= 0) {
      queryJson.put("from", opts.skip());
    }
    inject(queryJson, "search_before", opts.searchBefore());
    inject(queryJson, "search_after", opts.searchAfter());
    if (opts.explain() != null) {
      queryJson.put("explain", opts.explain());
    }
    if (opts.highlightStyle() != null) {
      ObjectNode highlight = Mapper.createObjectNode();
      if (opts.highlightStyle() != CoreHighlightStyle.SERVER_DEFAULT) {
        highlight.put("style", opts.highlightStyle().name().toLowerCase());
      }
      if (!opts.highlightFields().isEmpty()) {
        ArrayNode highlights = Mapper.createArrayNode();
        for (String s : opts.highlightFields()) {
          highlights.add(s);
        }
        highlight.set("fields", highlights);
      }
      queryJson.set("highlight", highlight);
    }
    if (!opts.fields().isEmpty()) {
      ArrayNode fields = Mapper.createArrayNode();
      for (String field : opts.fields()) {
        fields.add(field);
      }
      queryJson.set("fields", fields);
    }
    if (!opts.sort().isEmpty()) {
      ArrayNode sort = Mapper.createArrayNode();
      opts.sort().forEach(s -> sort.add(s.toJsonNode()));
      queryJson.set("sort", sort);
    }
    if (opts.disableScoring()) {
      queryJson.put("score", "none");
    }

    if (!opts.facets().isEmpty()) {
      ObjectNode f = Mapper.createObjectNode();
      for (Map.Entry<String, CoreSearchFacet> entry : opts.facets().entrySet()) {
        ObjectNode facetJson = Mapper.createObjectNode();
        entry.getValue().injectParams(facetJson);
        f.set(entry.getKey(), facetJson);
      }
      queryJson.set("facets", f);
    }

    ObjectNode control = Mapper.createObjectNode();
    control.put("timeout", timeout.toMillis());

    if (opts.consistency() != null && opts.consistency() != CoreSearchScanConsistency.NOT_BOUNDED) {
      ObjectNode consistencyJson = Mapper.createObjectNode();
      consistencyJson.put("level", opts.consistency().toString());
      control.set("consistency", consistencyJson);
    }

    if (opts.consistentWith() != null) {
      ObjectNode consistencyJson = Mapper.createObjectNode();
      consistencyJson.put("level", "at_plus");
      ObjectNode vectors = Mapper.createObjectNode();
      opts.consistentWith().tokens().forEach(token -> {
        String tokenKey = token.partitionID() + "/" + token.partitionUUID();
        vectors.put(tokenKey, token.sequenceNumber());
      });
      ObjectNode consistentWithJson = Mapper.createObjectNode();
      consistentWithJson.set(indexName, vectors);
      consistencyJson.set("vectors", consistentWithJson);
      control.set("consistency", consistencyJson);
    }

    if (!control.isEmpty()) {
      queryJson.set("ctl", control);
    }

    if (!opts.collections().isEmpty()) {
      ArrayNode collections = Mapper.createArrayNode();
      for (String collection : opts.collections()) {
        collections.add(collection);
      }
      queryJson.set("collections", collections);
    }

    if (opts.includeLocations() != null) {
      queryJson.put("includeLocations", opts.includeLocations());
    }

    JsonNode raw = opts.raw();
    if (raw != null) {
      for (Iterator<String> it = raw.fieldNames(); it.hasNext(); ) {
        String fieldName = it.next();
        queryJson.set(fieldName, raw.get(fieldName));
      }
    }

    if (disableShowRequest) {
      queryJson.put("showrequest", false);
    }
  }

  @Override
  public CoreAsyncResponse<CoreSearchResult> searchAsync(String indexName, CoreSearchRequest searchRequest, CoreSearchOptions options) {
    Duration timeout = options.commonOptions().timeout().orElse(core.environment().timeoutConfig().searchTimeout());
    return searchAsyncShared(searchRequestV2(indexName, searchRequest, options), searchRequest.vectorSearch != null, timeout);
  }

  @Override
  public Mono<CoreReactiveSearchResult> searchReactive(String indexName, CoreSearchRequest searchRequest, CoreSearchOptions options) {
    Duration timeout = options.commonOptions().timeout().orElse(core.environment().timeoutConfig().searchTimeout());
    return searchReactiveShared(searchRequestV2(indexName, searchRequest, options), searchRequest.vectorSearch != null, timeout);
  }

  private CoreAsyncResponse<CoreSearchResult> searchAsyncShared(ServerSearchRequest request, boolean requiresVectorIndexSupport, Duration timeout) {
    return preflightCheckScopedIndexes(timeout)
            .flatMap(ignore -> preflightCheckVectorIndexes(requiresVectorIndexSupport, timeout))
            .flatMap(ignore -> {
              core.send(request);
              return new CoreAsyncResponse<>(Mono.fromFuture(request.response())
                      .flatMap(response -> response.rows()
                              .map(CoreSearchRow::fromResponse)
                              .collectList()
                              .flatMap(rows -> response
                                      .trailer()
                                      .map(trailer -> new CoreSearchResult(rows, parseFacets(trailer), parseMeta(response, trailer)))
                              )
                      )
                      .doOnNext(ignored -> request.context().logicallyComplete())
                      .doOnError(err -> request.context().logicallyComplete(err))
                      .toFuture(), () -> {
              });
            });
  }

  public Mono<CoreReactiveSearchResult> searchReactiveShared(ServerSearchRequest request, boolean requiresVectorIndexSupport, Duration timeout) {
    return preflightCheckScopedIndexes(timeout).toMono()
            .then(Mono.defer(() -> preflightCheckVectorIndexes(requiresVectorIndexSupport, timeout).toMono()))
            .then(Mono.defer(() -> {
              core.send(request);
              return Mono.fromFuture(request.response())
                      .map(response -> {
                        Flux<CoreSearchRow> rows = response.rows().map(CoreSearchRow::fromResponse);
                        Mono<CoreSearchMetaData> meta = response.trailer().map(trailer -> parseMeta(response, trailer));
                        Mono<Map<String, CoreSearchFacetResult>> facets = response.trailer().map(ClassicCoreSearchOps::parseFacets);
                        return new CoreReactiveSearchResult(rows, facets, meta);

                      })
                      .doOnNext(ignored -> request.context().logicallyComplete())
                      .doOnError(err -> request.context().logicallyComplete(err));
            }));
  }

  private ServerSearchRequest searchRequestV2(String indexName, CoreSearchRequest searchRequest, CoreSearchOptions opts) {
    notNull(indexName, "indexName");
    Duration timeout = opts.commonOptions().timeout().orElse(environment().timeoutConfig().searchTimeout());

    ObjectNode topLevel = searchRequest.toJson();
    injectOptions(indexName, topLevel, timeout, opts, true);
    byte[] bytes = topLevel.toString().getBytes(StandardCharsets.UTF_8);

    RetryStrategy retryStrategy = opts.commonOptions().retryStrategy().orElse(environment().retryStrategy());

    RequestSpan span = coreResources()
            .requestTracer()
            .requestSpan(TracingIdentifiers.SPAN_REQUEST_SEARCH, opts.commonOptions().parentSpan().orElse(null));
    ServerSearchRequest request = new ServerSearchRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), indexName, bytes, span, scope);
    request.context().clientContext(opts.commonOptions().clientContext());
    return request;
  }
}
