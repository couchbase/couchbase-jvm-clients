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

import com.couchbase.client.core.msg.view.ViewResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Holds a the result of a View request operation if successful.
 *
 * @since 3.0.0
 */
public class ReactiveViewResult {

    /**
     * Holds the underlying view response from the core.
     */
    private final ViewResponse response;

    /**
     * Creates a new {@link ReactiveViewResult}.
     *
     * @param response the core response.
     */
    ReactiveViewResult(final ViewResponse response) {
        this.response = response;
    }

    /**
     * Returns the {@link ViewRow ViewRows} in a non-blocking, streaming fashion.
     *
     * @return the {@link Flux} of {@link ViewRow ViewRows}.
     */
    public Flux<ViewRow> rows() {
        return response.rows().map(r -> new ViewRow(r.data()));
    }

    /**
     * Returns the metadata associated with this {@link ReactiveViewResult}.
     *
     * @return the metadata associated.
     */
    public Mono<ViewMeta> meta() {
        return Mono.just(ViewMeta.from(response.header()));
    }

}
