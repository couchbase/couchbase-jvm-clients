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

package com.couchbase.client.java.view;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.msg.view.ViewResponse;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.query.QueryResult;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Internal helper to access and convert view requests and responses.
 *
 * @since 3.0.0
 */
@Stability.Internal
public class ViewAccessor {

    public static CompletableFuture<ViewResult> viewQueryAsync(final Core core, final ViewRequest request,
                                                               final JsonSerializer serializer) {
        return viewQueryInternal(core, request)
          .flatMap(response ->
            response.rows().collectList().map(rows -> new ViewResult(response.header(), rows, serializer))
          )
          .toFuture();
    }

    public static Mono<ReactiveViewResult> viewQueryReactive(final Core core, final ViewRequest request,
                                                             final JsonSerializer serializer) {
        return viewQueryInternal(core, request).map(r -> new ReactiveViewResult(r, serializer));
    }

    private static Mono<ViewResponse> viewQueryInternal(final Core core, final ViewRequest request) {
        core.send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .doFinally(signalType -> request.context().logicallyComplete());
    }

}
