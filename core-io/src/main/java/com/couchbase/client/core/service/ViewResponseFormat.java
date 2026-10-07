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
import com.couchbase.client.core.io.netty.view.ChunkedViewMessageHandler;
import com.couchbase.client.core.io.netty.view.ViewChunkResponseParser;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.core.msg.view.ViewChunkRow;
import com.couchbase.client.core.msg.view.ViewChunkTrailer;
import com.couchbase.client.core.msg.view.ViewError;
import com.couchbase.client.core.msg.view.ViewRequest;
import com.couchbase.client.core.msg.view.ViewResponse;
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
 * The view service's streamed JSON response, for {@link StreamingJsonResponseHandler}.
 * <p>
 * The header is complete when the total row count, the first row, or an error arrives,
 * or else at the end of the body.
 */
@NullMarked
final class ViewResponseFormat
  implements StreamingResponseFormat<ViewChunkHeader, ViewChunkRow, ViewChunkTrailer, ViewResponse> {

  static final ViewResponseFormat INSTANCE = new ViewResponseFormat();

  private ViewResponseFormat() {
  }

  @Override
  public String serviceName() {
    return "view";
  }

  @Override
  public ViewChunkTrailer parse(InputStream body, Consumer<ViewChunkHeader> onHeader, Consumer<byte[]> onRow) {
    return new Parser(onHeader, onRow).parse(body);
  }

  @Override
  public ViewChunkRow newRow(byte[] row) {
    return new ViewChunkRow(row);
  }

  @Override
  public ViewResponse newResponse(Request<ViewResponse> request, ViewChunkHeader header, Flux<ViewChunkRow> rows, Mono<ViewChunkTrailer> trailer) {
    return ((ViewRequest) request).decode(ResponseStatus.SUCCESS, header, rows, trailer);
  }

  @Override
  public Optional<CouchbaseException> error(ViewChunkTrailer trailer, int httpStatus, RequestContext requestContext) {
    return trailer.error().map(it -> ViewChunkResponseParser.errorToThrowable(it, HttpResponseStatus.valueOf(httpStatus), requestContext));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    return ChunkedViewMessageHandler.retryReason(error);
  }


  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   * <p>
   * Like the Netty-based parser, the header is complete when the total row count, the first row,
   * or an error arrives, or else at the end of the body (so a response always has a header).
   */
  private static final class Parser {
    private final Consumer<ViewChunkHeader> headerCallback;
    private final Consumer<byte[]> rowCallback;

    private boolean headerCompleted;
    private long totalRows;
    private byte @Nullable [] debug;
    private @Nullable String error;
    private @Nullable String reason;

    private Parser(Consumer<ViewChunkHeader> headerCallback, Consumer<byte[]> rowCallback) {
      this.headerCallback = requireNonNull(headerCallback);
      this.rowCallback = requireNonNull(rowCallback);
    }

    private ViewChunkTrailer parse(InputStream is) {
      try (JsonStreamParser parser = newStreamParser()) {
        parser.feed(is);
        parser.endOfInput();
      }
      maybeCompleteHeader();
      return new ViewChunkTrailer(
        error == null && reason == null
          ? Optional.empty()
          : Optional.of(new ViewError(error, reason))
      );
    }

    private JsonStreamParser newStreamParser() {
      return JsonStreamParser.builder()
        .doOnValue("/debug_info", v -> debug = v.bytes())
        .doOnValue("/total_rows", v -> {
          totalRows = v.readLong();
          maybeCompleteHeader();
        })
        .doOnValue("/rows/-", v -> {
          maybeCompleteHeader();
          rowCallback.accept(v.bytes());
        })
        .doOnValue("/error", v -> {
          maybeCompleteHeader();
          error = v.readString();
        })
        .doOnValue("/reason", v -> reason = v.readString())
        .build();
    }

    private void maybeCompleteHeader() {
      if (headerCompleted) return;
      headerCompleted = true;
      headerCallback.accept(new ViewChunkHeader(totalRows, Optional.ofNullable(debug)));
    }
  }
}
