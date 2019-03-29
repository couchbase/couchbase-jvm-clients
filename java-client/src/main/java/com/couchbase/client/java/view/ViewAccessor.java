package com.couchbase.client.java.view;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.view.ViewRequest;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

public class ViewAccessor {

    public static CompletableFuture<ViewResult> viewQueryAsync(final Core core, final ViewRequest request) {
        core.send(request);
        return request.response().thenApply(ViewResult::new);
    }

    public static Mono<ReactiveViewResult> viewQueryReactive(final Core core, final ViewRequest request) {
        core.send(request);
        return Reactor.wrap(request, request.response(), true).map(ReactiveViewResult::new);
    }
}
