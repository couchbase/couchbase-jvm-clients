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

package com.couchbase.client.core.io.netty.search;

import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.SearchErrorContext;
import com.couchbase.client.core.io.netty.chunk.ChunkedMessageHandler;
import com.couchbase.client.core.msg.search.*;
import com.couchbase.client.core.retry.RetryReason;

import java.util.Optional;

class ChunkedSearchMessageHandler
        extends ChunkedMessageHandler<SearchChunkHeader, SearchChunkRow, SearchChunkTrailer, SearchResponse, SearchRequest> {

    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    ChunkedSearchMessageHandler(BaseEndpoint endpoint, EndpointContext endpointContext) {
        super(endpoint, endpointContext, new SearchChunkResponseParser());
    }

    @Override
    protected Optional<RetryReason> qualifiesForRetry(final CouchbaseException exception) {
        if (exception.context() instanceof SearchErrorContext) {
            if (((SearchErrorContext) exception.context()).httpStatus() == HTTP_TOO_MANY_REQUESTS) {
                return Optional.of(RetryReason.SEARCH_TOO_MANY_REQUESTS);
            }
        }
        return Optional.empty();
    }

}

