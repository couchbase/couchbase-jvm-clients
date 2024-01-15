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

// [skip:<3.4.5]

import com.couchbase.client.java.search.SearchMetaData;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.search.result.SearchFacetResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.performer.core.stream.Streamer;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.search.StreamingSearchResult;
import com.couchbase.client.protocol.shared.ContentAs;
import com.couchbase.client.protocol.streams.Config;
import com.couchbase.client.protocol.streams.RequestItemsRequest;
import com.couchbase.search.SearchHelper;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.SignalType;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Allows streaming back a ReactiveSearchResult.
 */
public class ReactiveSearchResultStreamer extends Streamer<SearchRow> {
  private final ReactiveSearchResult result;
  private final AtomicReference<BaseSubscriber> rowsSubscriberRef = new AtomicReference<>();
  private final AtomicReference<BaseSubscriber> facetsSubscriberRef = new AtomicReference<>();
  private final AtomicReference<BaseSubscriber> metaSubscriberRef = new AtomicReference<>();

  public ReactiveSearchResultStreamer(ReactiveSearchResult result,
                                      PerRun perRun,
                                      String streamId,
                                      Config streamConfig,
                                      @Nullable ContentAs fieldsAs,
                                      Function<Throwable, com.couchbase.client.protocol.shared.Exception> convertException) {
    super(perRun, streamId, streamConfig, (row) -> {
      var converted = SearchHelper.convertRow(row, fieldsAs);
      return com.couchbase.client.protocol.run.Result.newBuilder()
              .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                      .setSearchStreamingResult(com.couchbase.client.protocol.sdk.search.StreamingSearchResult.newBuilder()
                              .setStreamId(streamId)
                              .setRow(converted)))
              .build();

    }, convertException);
    this.result = result;
  }

  @Override
  public boolean isCreated() {
    return rowsSubscriberRef.get() != null;
  }

  @Override
  public void run() {
    var rowsDone = new AtomicBoolean(false);
    var facetsDone = new AtomicBoolean(false);
    var metaDone = new AtomicBoolean(false);

    var rowsSubscriber = new BaseSubscriber<SearchRow>() {
      @Override
      protected void hookOnSubscribe(Subscription subscription) {
        if (streamConfig.hasAutomatically()) {
          request(Long.MAX_VALUE);
        } else if (!streamConfig.hasOnDemand()) {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      protected void hookOnNext(SearchRow value) {
        logger.info("Flux streamer {} sending one row", streamId);

        perRun.resultsStream().enqueue(convertResult.apply(value));
        streamed.incrementAndGet();
      }

      @Override
      protected void hookOnCancel() {
        handleCancel();
      }

      @Override
      protected void hookOnError(Throwable throwable) {
        logger.info("Flux streamer {} errored during rows with {}", streamId, throwable.toString());
        handleError(throwable);
      }

      @Override
      protected void hookFinally(SignalType type) {
        rowsDone.set(true);
      }
    };

    var facetsSubscriber = new BaseSubscriber<Map<String, SearchFacetResult>>() {
      // We don't do hookOnSubscribe as it doesn't make sense to try and request items from facets (which are 0 or 1).

      @Override
      protected void hookOnNext(Map<String, SearchFacetResult> value) {
        logger.info("Flux streamer {} sending facets", streamId);

        var facets = SearchHelper.convertFacets(value);

        perRun.resultsStream().enqueue(Result.newBuilder()
                .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                        .setSearchStreamingResult(StreamingSearchResult.newBuilder()
                                .setStreamId(streamId)
                                .setFacets(facets)))
                .build());
        streamed.incrementAndGet();
      }

      @Override
      protected void hookOnCancel() {
        handleCancel();
      }

      @Override
      protected void hookOnError(Throwable throwable) {
        logger.info("Flux streamer {} errored on facets with {}", streamId, throwable.toString());
        handleError(throwable);
      }

      @Override
      protected void hookFinally(SignalType type) {
        facetsDone.set(true);
      }
    };

    var metaSubscriber = new BaseSubscriber<SearchMetaData>() {
      // We don't do hookOnSubscribe as it doesn't make sense to try and request items from meta (which are 0 or 1).

      @Override
      protected void hookOnNext(SearchMetaData value) {
        logger.info("Flux streamer {} sending meta", streamId);

        var converted = SearchHelper.convertMetaData(value);
        perRun.resultsStream().enqueue(Result.newBuilder()
                .setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                        .setSearchStreamingResult(StreamingSearchResult.newBuilder()
                                .setStreamId(streamId)
                                .setMetaData(converted)))
                .build());
        streamed.incrementAndGet();
      }

      @Override
      protected void hookOnCancel() {
        handleCancel();
      }

      @Override
      protected void hookOnError(Throwable throwable) {
        logger.info("Flux streamer {} errored on facets with {}", streamId, throwable.toString());
        handleError(throwable);
      }

      @Override
      protected void hookFinally(SignalType type) {
        facetsDone.set(true);
      }
    };

    result.rows().subscribe(rowsSubscriber);
    result.facets().subscribe(facetsSubscriber);
    result.metaData().subscribe(metaSubscriber);

    rowsSubscriberRef.set(rowsSubscriber);
    facetsSubscriberRef.set(facetsSubscriber);
    metaSubscriberRef.set(metaSubscriber);

    logger.info("Waiting for flux streamer {}", streamId);

    while (!rowsDone.get() && !facetsDone.get() && !metaDone.get()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    perRun.resultsStream().enqueue(Result.newBuilder()
            .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setComplete(com.couchbase.client.protocol.streams.Complete.newBuilder().setStreamId(streamId)))
            .build());

    logger.info("Flux streamer {} has ended", streamId);
  }

  private void handleError(Throwable throwable) {
    perRun.resultsStream().enqueue(Result.newBuilder()
            .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setError(com.couchbase.client.protocol.streams.Error.newBuilder()
                            .setException(convertException.apply(throwable))
                            .setStreamId(streamId)))
            .build());
  }

  private void handleCancel() {
    perRun.resultsStream().enqueue(Result.newBuilder()
            .setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                    .setCancelled(com.couchbase.client.protocol.streams.Cancelled.newBuilder().setStreamId(streamId)))
            .build());

    logger.info("Flux streamer {} has finished being cancelled after streaming back {} results", streamId, streamed.get());
  }

  @Override
  public void cancel() {
    rowsSubscriberRef.get().cancel();
    facetsSubscriberRef.get().cancel();
    metaSubscriberRef.get().cancel();
  }

  @Override
  public void requestItems(RequestItemsRequest request) {
    rowsSubscriberRef.get().request(request.getNumItems());
  }
}