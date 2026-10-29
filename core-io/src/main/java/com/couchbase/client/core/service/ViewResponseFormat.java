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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.ViewNotFoundException;
import com.couchbase.client.core.error.context.ViewErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
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
    return trailer.error().map(it -> errorToThrowable(it, httpStatus, requestContext));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    if (!(error.context() instanceof ViewErrorContext)) {
      return Optional.empty();
    }
    return shouldRetry((ViewErrorContext) error.context());
  }

  /**
   * Analyses status codes and checks if a retry needs to happen.
   *
   * Some status codes are ambiguous, so their contents are inspected further.
   *
   * @param context the error context with information.
   * @return true if retry is needed, false otherwise.
   */
  private static Optional<RetryReason> shouldRetry(ViewErrorContext context) {
    switch (context.httpStatus()) {
      case 404:
        return analyse404Response(context.error());
      case 500:
        return analyse500Response(context.error());
      case 302:
        return Optional.of(RetryReason.VIEWS_NO_ACTIVE_PARTITION);
      case 300:
      case 301:
      case 303:
      case 307:
      case 401:
      case 408:
      case 409:
      case 412:
      case 416:
      case 417:
      case 501:
      case 502:
      case 503:
      case 504:
        return Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE);
      default:
        return Optional.empty();
    }
  }

  /**
   * Analyses the content of a 404 response to see if it is legible for retry.
   *
   * If the content contains ""reason":"missing"", it is a clear indication that the responding node
   * is unprovisioned and therefore it should be retried. All other cases indicate a provisioned node,
   * but the design document/view is not found, which should not be retried.
   *
   * @param error the parsed error content.
   * @return true if it needs to be retried, false otherwise.
   */
  private static Optional<RetryReason> analyse404Response(ViewError error) {
    return error.reason().equals("missing") ? Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE) : Optional.empty();
  }

  /**
   * Analyses the content of a 500 response to see if it is legible for retry.
   *
   * @param error the parsed error content.
   * @return true if it needs to be retried, false otherwise.
   */
  private static Optional<RetryReason> analyse500Response(ViewError error) {
    if (error.reason().contains("{not_found, missing_named_view}") || error.reason().contains("badarg")) {
      return Optional.empty();
    }
    return Optional.of(RetryReason.VIEWS_TEMPORARY_FAILURE);
  }

  /**
   * Returns the exception for the {@code error} and {@code reason} fields of a view query response.
   */
  static CouchbaseException errorToThrowable(ViewError e, int httpStatus, RequestContext requestContext) {
    ResponseStatus responseStatus = HttpProtocol.decodeStatus(httpStatus);
    ViewErrorContext errorContext = new ViewErrorContext(responseStatus, requestContext, e, httpStatus);
    if (responseStatus == ResponseStatus.NOT_FOUND || e.error().equals("not_found") || e.reason().contains("not_found")) {
      return new ViewNotFoundException(errorContext);
    }
    if (httpStatus == 400) {
      return new InvalidArgumentException("View service reported an invalid argument: " + e, null, errorContext);
    }
    return new CouchbaseException("Unknown view error: " + e.toString(), errorContext);
  }


  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   * <p>
   * The header is complete when the total row count, the first row,
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
