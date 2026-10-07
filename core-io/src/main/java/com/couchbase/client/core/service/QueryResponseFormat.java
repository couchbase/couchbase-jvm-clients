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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.netty.query.QueryMessageHandler;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryReason;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Consumer;

import static com.couchbase.client.core.io.netty.query.QueryChunkResponseParser.errorsToThrowable;
import static java.util.Objects.requireNonNull;

/**
 * The query service's streamed JSON response, for {@link StreamingJsonResponseHandler}.
 * <p>
 * The header is complete when the first row arrives, or the status if there are no rows.
 */
@NullMarked
final class QueryResponseFormat
  implements StreamingResponseFormat<QueryChunkHeader, QueryChunkRow, QueryChunkTrailer, QueryResponse> {

  static final QueryResponseFormat INSTANCE = new QueryResponseFormat();

  private QueryResponseFormat() {
  }

  @Override
  public String serviceName() {
    return "query";
  }

  @Override
  public QueryChunkTrailer parse(InputStream body, Consumer<QueryChunkHeader> onHeader, Consumer<byte[]> onRow) {
    return new Parser(onHeader, onRow).parse(body);
  }

  @Override
  public QueryChunkRow newRow(byte[] row) {
    return new QueryChunkRow(row);
  }

  @Override
  public QueryResponse newResponse(Request<QueryResponse> request, QueryChunkHeader header, Flux<QueryChunkRow> rows, Mono<QueryChunkTrailer> trailer) {
    return ((QueryRequest) request).decode(ResponseStatus.SUCCESS, header, rows, trailer);
  }

  @Override
  public Optional<CouchbaseException> error(QueryChunkTrailer trailer, int httpStatus, RequestContext requestContext) {
    return trailer.errors().map(it -> errorsToThrowable(it, httpStatus, requestContext));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    return QueryMessageHandler.qualifiesForRetry(error.context());
  }


  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   */
  private static final class Parser {
    private final Consumer<QueryChunkHeader> headerCallback;
    private final Consumer<byte[]> rowCallback;

    private boolean headerCompleted = false;

    private @Nullable String requestId;
    private byte @Nullable [] signature;
    private @Nullable String clientContextId;
    private @Nullable String prepared;

    private @Nullable String status;
    private byte @Nullable [] metrics;
    private byte @Nullable [] warnings;
    private byte @Nullable [] errors;
    private byte @Nullable [] profile;

    private Parser(Consumer<QueryChunkHeader> headerCallback, Consumer<byte[]> rowCallback) {
      this.headerCallback = requireNonNull(headerCallback);
      this.rowCallback = requireNonNull(rowCallback);
    }

    private QueryChunkTrailer parse(InputStream is) {
      try (JsonStreamParser parser = newStreamParser()) {
        parser.feed(is);
        parser.endOfInput();
      }
      return trailer();
    }

    private JsonStreamParser newStreamParser() {
      return JsonStreamParser.builder()
        .doOnValue("/requestID", v -> requestId = v.readString())
        .doOnValue("/signature", v -> signature = v.bytes())
        .doOnValue("/clientContextID", v -> clientContextId = v.readString())
        .doOnValue("/prepared", v -> prepared = v.readString())
        .doOnValue("/results/-", v -> {
          maybeCompleteHeader();
          rowCallback.accept(v.bytes());
        })
        .doOnValue("/status", v -> {
          maybeCompleteHeader();
          status = v.readString();
        })
        .doOnValue("/metrics", v -> metrics = v.bytes())
        .doOnValue("/profile", v -> profile = v.bytes())
        .doOnValue("/errors", v -> errors = v.bytes())
        .doOnValue("/warnings", v -> warnings = v.bytes())
        .build();
    }

    private void maybeCompleteHeader() {
      if (headerCompleted) return;

      headerCompleted = true;

      QueryChunkHeader header = new QueryChunkHeader(
        requestId,
        Optional.ofNullable(clientContextId),
        Optional.ofNullable(signature),
        Optional.ofNullable(prepared)
      );

      headerCallback.accept(header);
    }

    private QueryChunkTrailer trailer() {
      return new QueryChunkTrailer(
        status,
        Optional.ofNullable(metrics),
        Optional.ofNullable(warnings),
        Optional.ofNullable(errors),
        Optional.ofNullable(profile)
      );
    }
  }
}
