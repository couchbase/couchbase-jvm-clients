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
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.core.msg.search.SearchResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchMetrics;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchStatus;
import com.couchbase.client.java.search.result.impl.DefaultSearchMetrics;
import com.couchbase.client.java.search.result.impl.DefaultSearchStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Internal helper to access and convert view requests and responses.
 *
 * @since 3.0.0
 */
@Stability.Internal
public class SearchAccessor {

    // TODO facets are streaming (update: they're not, but they need adding)

    public static CompletableFuture<SearchResult> searchQueryAsync(final Core core, final SearchRequest request) {
        core.send(request);
        return Mono.fromFuture(request.response())

                .flatMap(response -> response.rows()
                        .map(row -> SearchQueryRow.fromResponse(row))
                        .collectList()

                        .flatMap(rows -> response.trailer()
                                .map(trailer -> {
                                    byte[] rawStatus = response.header().getStatus();
                                    List<RuntimeException> errors = SearchAccessor.parseErrors(rawStatus);
                                    SearchMeta meta = parseMeta(response, trailer);

                                    return new SearchResult(rows, errors, meta);
                                })
                        )
                )

                .toFuture();
    }

    static SearchMeta parseMeta(SearchResponse response, SearchChunkTrailer trailer) {
        byte[] rawStatus = response.header().getStatus();
        SearchStatus status = DefaultSearchStatus.fromBytes(rawStatus);
        SearchMetrics metrics = new DefaultSearchMetrics(trailer.took(), trailer.totalRows(), trailer.maxScore());
        SearchMeta meta = new SearchMeta(status, metrics);
        return meta;
    }

    public static Mono<ReactiveSearchResult> searchQueryReactive(final Core core, final SearchRequest request) {
        core.send(request);
        return Mono.fromFuture(request.response())

                .map(response -> {
                    byte[] rawStatus = response.header().getStatus();
                    SearchStatus status = DefaultSearchStatus.fromBytes(rawStatus);


                    // Any errors should be raised in SearchServiceException and will be returned directly
                    Flux<SearchQueryRow> rows = response.rows()
                            .map(row -> SearchQueryRow.fromResponse(row));

                    Mono<SearchMeta> meta = response.trailer()
                            .map(trailer -> {
                                SearchMetrics metrics = new DefaultSearchMetrics(trailer.took(), trailer.totalRows(), trailer.maxScore());

                                return new SearchMeta(status, metrics);
                            });

                    return new ReactiveSearchResult(rows, meta);
                });
    }

    static List<RuntimeException> parseErrors(final byte[] status) {
        try {
            JsonObject jsonStatus = JacksonTransformers.MAPPER.readValue(status, JsonObject.class);

            Object errorsRaw = jsonStatus.get("errors");

            List<RuntimeException> exceptions = new ArrayList<>();

            if (errorsRaw instanceof JsonArray) {
                JsonArray errorsJson = (JsonArray) errorsRaw;
                for (Object o : errorsJson) {
                    exceptions.add(new RuntimeException(String.valueOf(o)));
                }
            } else if (errorsRaw instanceof JsonObject) {
                JsonObject errorsJson = (JsonObject) errorsRaw;
                for (String key : errorsJson.getNames()) {
                    exceptions.add(new RuntimeException(key + ": " + errorsJson.get(key)));
                }
            }

            return exceptions;

        } catch (IOException e) {
            throw new DecodingFailedException("Failed to decode row '" + new String(status, UTF_8) + "'", e);
        }
    }

}
