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

package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.chunk.ChunkedResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SearchResponse extends BaseResponse
        implements ChunkedResponse<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer> {

    private final SearchChunkHeader header;
    private final Flux<SearchChunkRow> rows;
    private final Mono<SearchChunkTrailer> trailer;

    SearchResponse(ResponseStatus status, SearchChunkHeader header, Flux<SearchChunkRow> rows,
                   Mono<SearchChunkTrailer> trailer) {
        super(status);
        this.header = header;
        this.rows = rows;
        this.trailer = trailer;
    }

    @Override
    public SearchChunkHeader header() {
        return header;
    }

    @Override
    public Flux<SearchChunkRow> rows() {
        return rows;
    }

    @Override
    public Mono<SearchChunkTrailer> trailer() {
        return trailer;
    }
}
