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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * Protects against concurrent op cancellation.
 * <p>
 * In reactive, if have multiple concurrent ops, if one fails then all the others will just stop executing once the
 * error is raised past them in the chain.  As if concurrent ops are being done with concatMap/flatMap, once the FluxConcatMap
 * operator gets onError, it calls cancel() on its internal stream.
 * <p>
 * To avoid this, on a ctx.insert(), a MonoBridge is returned to the lambda and the actual work is started on a separate
 * chain, whose signals are forwarded to the MonoBridge.  That chain is then protected from any cancellation events, which
 * the MonoBridge will receive.
 * <p>
 * Since this work, a CANCEL signal should not appear internally, and is possible indicative of an internal bug if it does.
 */
@Stability.Internal
public class MonoBridge<T> {
    // If the original stream is cancelled, don't want to raise any signals on it as it'll lead to onErrorDropped
    private boolean done;
    private final Sinks.One<T> actual = Sinks.one();
    private final Mono<T> external = actual.asMono();

    public MonoBridge(Mono<T> feedFrom, String dbg, Object syncer, @Nullable CoreTransactionLogger logger) {
        Objects.requireNonNull(feedFrom);
        Objects.requireNonNull(dbg);
        Objects.requireNonNull(syncer);

        feedFrom
                .onErrorResume(err -> {
                    // Thread safety 10.1. Prevents raising concurrent errors which will lead to onErrorDropped.
                    // Raising an error will cause all connected chains to be cancelled, still under the synchronized, which
                    // prevents any connected MonoBridges from raising subsequent errors.

                    synchronized (syncer) {
                        if (!done) {
                            if (logger != null) logger.info("", "MB: [%s] propagating err %s", dbg, err.toString());
                            actual.tryEmitError(err).orThrow();
                        } else if (logger != null)
                            logger.info("", "MB: [%s] skipping err propagating as done", dbg);
                        return Mono.empty();
                    }
                })

                // Subscribe so this work will be happening asynchronously.  Should already be publishing on our thread-pool.
                .subscribe(next -> {
                            if (!done) {
                                if (logger != null) logger.info("", "MB: [%s] propagating next", dbg);
                                actual.tryEmitValue(next).orThrow();
                            } else if (logger != null) logger.info("", "MB: [%s] skipping next propagating as done", dbg);
                        },
                        err -> {
                            // Should not trigger due to onErrorResume
                            throw new IllegalStateException("Should not reach MonoBridge error producer");
                        },
                        () -> {
                            // Even though a Mono still need to propagate onComplete in case it's Mono<Void>
                            if (!done) {
                                if (logger != null) logger.info("", "MB: [%s] propagating complete", dbg);
                                actual.tryEmitEmpty();
                            } else if (logger != null)
                                logger.info("", "MB: [%s] skipping complete propagating as done", dbg);
                        });

        external.doOnCancel(() -> {
                    if (logger != null) logger.info("", "MB: [%s] is cancelled", dbg);
                    done = true;
                })
                .doOnTerminate(() -> {
                    if (logger != null) logger.info("", "MB: [%s] is errored or complete", dbg);
                    done = true;
                });
    }

    public Mono<T> external() {
        return external;
    }
}
