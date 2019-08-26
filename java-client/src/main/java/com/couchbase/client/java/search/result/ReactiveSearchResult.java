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

import com.couchbase.client.java.search.SearchMetaData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveSearchResult {

    private final Flux<SearchQueryRow> rows;
    private final Mono<SearchMetaData> meta;

    public ReactiveSearchResult(Flux<SearchQueryRow> rows,
                                Mono<SearchMetaData> meta) {
        this.rows = rows;
        this.meta = meta;
    }

    /**
     * The list of FTS result rows for the FTS query, in the form of a reactive {@link Flux} publisher.
     * <p>
     * Any errors will be raised as onError on this.
     */
    public Flux<SearchQueryRow> rows() {
        return rows;
    }

    /**
     * Any additional meta information associated with the FTS query, in the form of a reactive {@link Mono} publisher.
     */
    public Mono<SearchMetaData> metaData() {
        return meta;
    }
}
