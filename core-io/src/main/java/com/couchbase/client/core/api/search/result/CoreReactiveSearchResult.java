/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchMetaData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@Stability.Internal
public class CoreReactiveSearchResult {

    private final Flux<CoreSearchRow> rows;
    private final Mono<Map<String, CoreSearchFacetResult>> facets;
    private final Mono<CoreSearchMetaData> meta;

    public CoreReactiveSearchResult(Flux<CoreSearchRow> rows, Mono<Map<String, CoreSearchFacetResult>> facets, Mono<CoreSearchMetaData> meta) {
        this.rows = rows;
        this.facets = facets;
        this.meta = meta;
    }

    public Flux<CoreSearchRow> rows() {
        return rows;
    }

    public Mono<CoreSearchMetaData> metaData() {
        return meta;
    }

    public Mono<Map<String, CoreSearchFacetResult>> facets() {
        return facets;
    }
}
