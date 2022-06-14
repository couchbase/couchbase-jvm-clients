/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.threadlocal;

import com.couchbase.client.core.annotation.Stability;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * Used to manage ThreadLocalStorage (or reactive context) of TransactionMarker.
 */
@Stability.Internal
public class TransactionMarkerOwner {
    private static final ThreadLocal<TransactionMarker> marker = new ThreadLocal<TransactionMarker>();

    public static void set(TransactionMarker toInject) {
        if (marker.get() != null) {
            throw new IllegalStateException("Trying to set transaction context when already inside a transaction - likely an internal bug, please report it");
        }
        marker.set(toInject);
    }

    public static void clear() {
        marker.remove();
    }

    public static Mono<Optional<TransactionMarker>> get() {
        return Mono.deferContextual(ctx -> {
            TransactionMarker fromThreadLocal = marker.get();
            TransactionMarker fromReactive = ctx.hasKey(TransactionMarker.class) ? ctx.get(TransactionMarker.class) : null;

            if (fromThreadLocal != null) {
                return Mono.just(Optional.of(fromThreadLocal));
            }
            if (fromReactive != null) {
                return Mono.just(Optional.of(fromReactive));
            }
            return Mono.just(Optional.empty());
        });
    }
}
