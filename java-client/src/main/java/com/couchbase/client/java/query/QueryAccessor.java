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

package com.couchbase.client.java.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class QueryAccessor {

    public static CompletableFuture<QueryResult> queryAsync(final Core core, final QueryRequest request) {
        return queryInternal(core, request)
          .flatMap(response -> response
            .rows()
            .collectList()
            .flatMap(rows -> response
                .trailer()
                .map(trailer -> new QueryResult(response.header(), rows, trailer))
            )
          )
          .toFuture();
    }

    public static Mono<ReactiveQueryResult> queryReactive(final Core core,
                                                          final QueryRequest request) {
        return queryInternal(core, request).map(ReactiveQueryResult::new);
    }

    private static Mono<QueryResponse> queryInternal(final Core core, final QueryRequest request) {
        core.send(request);
        return Reactor.wrap(request, request.response(), true);
    }

}
