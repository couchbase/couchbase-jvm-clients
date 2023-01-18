/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.stream;

import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.Streamer;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.streams.Config;
import com.couchbase.client.protocol.streams.RequestItemsRequest;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Allows streaming back a Flux.
 */
public class FluxStreamer<T> extends Streamer<T> {
    private final Flux<T> results;
    private final AtomicReference<BaseSubscriber> subscriberRef = new AtomicReference<>();

    public FluxStreamer(Flux<T> results,
                        PerRun perRun,
                        String streamId,
                        Config streamConfig,
                        Function<T, Result> convertResult,
                        Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException) {
        super(perRun, streamId, streamConfig, convertResult, convertException);
        this.results = results;
    }

    @Override
    public boolean isCreated() {
        return subscriberRef.get() != null;
    }

    @Override
    public void run() {
        var done = new AtomicBoolean(false);

        var subscriber = new BaseSubscriber<T>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                if (streamConfig.hasAutomatically()) {
                    request(Long.MAX_VALUE);
                } else if (!streamConfig.hasOnDemand()) {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            protected void hookOnNext(T value) {
                logger.info("Flux streamer {} sending one", streamId);

                perRun.resultsStream().enqueue(convertResult.apply(value));
                streamed.incrementAndGet();
            }

            @Override
            protected void hookOnComplete() {
                perRun.resultsStream().enqueue(Result.newBuilder()
                        .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                                .setComplete(com.couchbase.client.protocol.streams.Complete.newBuilder().setStreamId(streamId)))
                        .build());

                logger.info("Flux streamer {} has finished streaming back {} results", streamId, streamed.get());
            }

            @Override
            protected void hookOnCancel() {
                perRun.resultsStream().enqueue(Result.newBuilder()
                        .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                                .setCancelled(com.couchbase.client.protocol.streams.Cancelled.newBuilder().setStreamId(streamId)))
                        .build());

                logger.info("Flux streamer {} has finished being cancelled after streaming back {} results", streamId, streamed.get());
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                logger.info("Flux streamer {} errored with {}", streamId, throwable.toString());

                perRun.resultsStream().enqueue(Result.newBuilder()
                        .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                                .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                                        .setException(convertException.apply(throwable))
                                        .setStreamId(streamId)))
                        .build());
            }

            @Override
            protected void hookFinally(SignalType type) {
                done.set(true);
            }
        };
        results.subscribe(subscriber);
        subscriberRef.set(subscriber);

        logger.info("Waiting for flux streamer {}", streamId);

        while (!done.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        logger.info("Flux streamer {} has ended", streamId);
    }

    @Override
    public void cancel() {
        subscriberRef.get().cancel();
    }

    @Override
    public void requestItems(RequestItemsRequest request) {
        subscriberRef.get().request(request.getNumItems());
    }
}
