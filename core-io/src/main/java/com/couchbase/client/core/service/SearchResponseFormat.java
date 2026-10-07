/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.netty.search.ChunkedSearchMessageHandler;
import com.couchbase.client.core.io.netty.search.SearchChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.search.SearchChunkHeader;
import com.couchbase.client.core.msg.search.SearchChunkRow;
import com.couchbase.client.core.msg.search.SearchChunkTrailer;
import com.couchbase.client.core.msg.search.SearchResponse;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.retry.RetryReason;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * The search service's streamed JSON response, for {@link StreamingJsonResponseHandler}.
 * <p>
 * The header is complete when the status arrives.
 */
@NullMarked
final class SearchResponseFormat
  implements StreamingResponseFormat<SearchChunkHeader, SearchChunkRow, SearchResponseFormat.Result, SearchResponse> {

  static final SearchResponseFormat INSTANCE = new SearchResponseFormat();

  private SearchResponseFormat() {
  }

  @Override
  public String serviceName() {
    return "search";
  }

  @Override
  public Result parse(InputStream body, Consumer<SearchChunkHeader> onHeader, Consumer<byte[]> onRow) {
    return new Parser(onHeader, onRow).parse(body);
  }

  @Override
  public SearchChunkRow newRow(byte[] row) {
    return new SearchChunkRow(row);
  }

  @Override
  public SearchResponse newResponse(Request<SearchResponse> request, SearchChunkHeader header, Flux<SearchChunkRow> rows, Mono<Result> trailer) {
    return ((ServerSearchRequest) request).decode(ResponseStatus.SUCCESS, header, rows, trailer.map(it -> it.trailer));
  }

  @Override
  public Optional<CouchbaseException> error(Result trailer, int httpStatus, RequestContext requestContext) {
    return Optional.ofNullable(trailer.error)
      .map(it -> SearchChunkResponseParser.errorsToThrowable(it, HttpResponseStatus.valueOf(httpStatus), requestContext));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    return ChunkedSearchMessageHandler.retryReason(error);
  }


  /**
   * The trailer, plus the response's {@code error} field (if any), which the trailer doesn't have room for.
   */
  static final class Result {
    final SearchChunkTrailer trailer;
    final byte @Nullable [] error;

    Result(SearchChunkTrailer trailer, byte @Nullable [] error) {
      this.trailer = requireNonNull(trailer);
      this.error = error;
    }
  }

  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   * <p>
   * Like the Netty-based parser, the header is complete when the {@code status} field arrives.
   * The search service sends it first, before the hits.
   */
  private static final class Parser {
    private final Consumer<SearchChunkHeader> headerCallback;
    private final Consumer<byte[]> rowCallback;

    private boolean headerCompleted;
    private byte @Nullable [] error;
    private byte @Nullable [] facets;
    private long totalRows;
    private double maxScore;
    private long took;

    private Parser(Consumer<SearchChunkHeader> headerCallback, Consumer<byte[]> rowCallback) {
      this.headerCallback = requireNonNull(headerCallback);
      this.rowCallback = requireNonNull(rowCallback);
    }

    private Result parse(InputStream is) {
      try (JsonStreamParser parser = newStreamParser()) {
        parser.feed(is);
        parser.endOfInput();
      }
      return new Result(new SearchChunkTrailer(totalRows, maxScore, took, facets), error);
    }

    private JsonStreamParser newStreamParser() {
      return JsonStreamParser.builder()
        .doOnValue("/status", v -> {
          if (!headerCompleted) {
            headerCompleted = true;
            headerCallback.accept(new SearchChunkHeader(v.bytes()));
          }
        })
        .doOnValue("/error", v -> error = v.bytes())
        .doOnValue("/hits/-", v -> rowCallback.accept(v.bytes()))
        .doOnValue("/total_hits", v -> totalRows = v.readLong())
        .doOnValue("/max_score", v -> maxScore = v.readDouble())
        .doOnValue("/took", v -> took = v.readLong())
        .doOnValue("/facets", v -> facets = v.bytes())
        .build();
    }
  }
}
