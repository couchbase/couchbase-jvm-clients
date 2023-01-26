/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.core.msg.search.SearchResponse;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.search.result.DateRangeSearchFacetResult;
import com.couchbase.client.java.search.result.NumericRangeSearchFacetResult;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchFacetResult;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.result.SearchStatus;
import com.couchbase.client.java.search.result.TermSearchFacetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Internal helper to access and convert view requests and responses.
 *
 * @since 3.0.0
 */
@Stability.Internal
public class SearchAccessor {

    private static final byte[] NULL = new byte[] { 'n', 'u', 'l', 'l' };

    public static CompletableFuture<SearchResult> searchQueryAsync(final Core core, final SearchRequest request,
                                                                   final JsonSerializer serializer) {
        checkIfProtostellar(core);
        core.send(request);
        return Mono.fromFuture(request.response())
          .flatMap(response -> response.rows()
              .map(row -> SearchRow.fromResponse(row, serializer))
              .collectList()
              .flatMap(rows -> response
                .trailer()
                .map(trailer -> new SearchResult(rows, parseFacets(trailer), parseMeta(response, trailer)))
              )
          )
          .doOnNext(ignored -> request.context().logicallyComplete())
          .doOnError(err -> request.context().logicallyComplete(err))
          .toFuture();
    }

    public static Mono<ReactiveSearchResult> searchQueryReactive(final Core core, final SearchRequest request,
                                                                 final JsonSerializer serializer) {
        checkIfProtostellar(core);
        core.send(request);
        return Mono
          .fromFuture(request.response())
          .map(response -> {
            Flux<SearchRow> rows = response.rows().map(row -> SearchRow.fromResponse(row, serializer));
            Mono<SearchMetaData> meta = response.trailer().map(trailer -> parseMeta(response, trailer));
            Mono<Map<String, SearchFacetResult>> facets = response.trailer().map(SearchAccessor::parseFacets);
            return new ReactiveSearchResult(rows, facets, meta);
          })
          .doOnNext(ignored -> request.context().logicallyComplete())
          .doOnError(err -> request.context().logicallyComplete(err));
    }

    private static Map<String, SearchFacetResult> parseFacets(final SearchChunkTrailer trailer) {
      byte[] rawFacets = trailer.facets();
      if (rawFacets == null || rawFacets.length == 0 || Arrays.equals(rawFacets, NULL)) {
        return Collections.emptyMap();
      }

      ObjectNode objectNode = (ObjectNode) Mapper.decodeIntoTree(rawFacets);
      Map<String, SearchFacetResult> facets = new HashMap<>();

      forEachField(objectNode, (facetName, facetEntry) -> {
        // Copy the facet name into the facet JSON node to simplify data binding
        ((ObjectNode) facetEntry).set("$name", new TextNode(facetName));

        if (facetEntry.has("numeric_ranges")) {
          facets.put(facetName, Mapper.convertValue(facetEntry, NumericRangeSearchFacetResult.class));
        } else if (facetEntry.has("date_ranges")) {
          facets.put(facetName, Mapper.convertValue(facetEntry, DateRangeSearchFacetResult.class));
        } else {
          facets.put(facetName, Mapper.convertValue(facetEntry, TermSearchFacetResult.class));
        }
      });

      return facets;
    }

    private static void forEachField(ObjectNode node, BiConsumer<String, JsonNode> consumer) {
      requireNonNull(consumer);
      node.fields().forEachRemaining(entry -> consumer.accept(entry.getKey(), entry.getValue()));
    }

    private static SearchMetaData parseMeta(final SearchResponse response, final SearchChunkTrailer trailer) {
        SearchStatus status = Mapper.decodeInto(response.header().getStatus(), SearchStatus.class);
        SearchMetrics metrics = new SearchMetrics(
          trailer.took(),
          trailer.totalRows(),
          trailer.maxScore(),
          status.successCount(),
          status.errorCount()
        );
        return new SearchMetaData(status.errors(), metrics);
    }

    private static void checkIfProtostellar(Core core) {
        if (core.isProtostellar()) {
           throw CoreProtostellarUtil.unsupportedInProtostellar("search");
        }
    }
}
