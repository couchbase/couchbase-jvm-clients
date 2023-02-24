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

package com.couchbase.client.java.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreReactiveSearchResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.search.SearchMetaData;
import com.couchbase.client.java.search.util.SearchFacetUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class ReactiveSearchResult {

    private final CoreReactiveSearchResult internal;
    private final JsonSerializer serializer;

    @Stability.Internal
    public ReactiveSearchResult(CoreReactiveSearchResult internal, JsonSerializer serializer) {
        this.internal = internal;
        this.serializer = serializer;
    }

    /**
     * The list of FTS result rows for the FTS query, in the form of a reactive {@link Flux} publisher.
     * <p>
     * Any errors will be raised as onError on this.
     */
    public Flux<SearchRow> rows() {
        return internal.rows().map(row -> new SearchRow(row, serializer));
    }

    /**
     * Any additional meta information associated with the FTS query, in the form of a reactive {@link Mono} publisher.
     */
    public Mono<SearchMetaData> metaData() {
        return internal.metaData().map(SearchMetaData::new);
    }

    public Mono<Map<String, SearchFacetResult>> facets() {
        return internal.facets().map(result -> {
            Map<String, SearchFacetResult> out = new HashMap<>();
            result.forEach((k, v) -> out.put(k, SearchFacetUtil.convert(v)));
            return out;
        });
    }
}
