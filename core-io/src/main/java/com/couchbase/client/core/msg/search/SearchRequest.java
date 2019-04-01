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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.HttpRequest;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class SearchRequest extends BaseRequest<SearchResponse>
        implements HttpRequest<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer, SearchResponse> {

    public SearchRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy) {
        super(timeout, ctx, retryStrategy);
    }

    @Override
    public FullHttpRequest encode() {
        return null;
    }

    @Override
    public ServiceType serviceType() {
        return ServiceType.SEARCH;
    }

    @Override
    public SearchResponse decode(ResponseStatus status, SearchChunkHeader header, Flux<SearchChunkRow> rows,
                                 Mono<SearchChunkTrailer> trailer) {
        return new SearchResponse(status, header, rows, trailer);
    }
}
