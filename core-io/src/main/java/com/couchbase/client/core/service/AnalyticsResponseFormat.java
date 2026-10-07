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
import com.couchbase.client.core.io.netty.analytics.AnalyticsChunkResponseParser;
import com.couchbase.client.core.io.netty.analytics.AnalyticsMessageHandler;
import com.couchbase.client.core.json.stream.JsonStreamParser;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
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
 * The analytics service's streamed JSON response, for {@link StreamingJsonResponseHandler}.
 * <p>
 * The header is complete when the first row, the status, or the metrics arrive.
 */
@NullMarked
final class AnalyticsResponseFormat
  implements StreamingResponseFormat<AnalyticsChunkHeader, AnalyticsChunkRow, AnalyticsChunkTrailer, AnalyticsResponse> {

  static final AnalyticsResponseFormat INSTANCE = new AnalyticsResponseFormat();

  private AnalyticsResponseFormat() {
  }

  @Override
  public String serviceName() {
    return "analytics";
  }

  @Override
  public AnalyticsChunkTrailer parse(InputStream body, Consumer<AnalyticsChunkHeader> onHeader, Consumer<byte[]> onRow) {
    return new Parser(onHeader, onRow).parse(body);
  }

  @Override
  public AnalyticsChunkRow newRow(byte[] row) {
    return new AnalyticsChunkRow(row);
  }

  @Override
  public AnalyticsResponse newResponse(Request<AnalyticsResponse> request, AnalyticsChunkHeader header, Flux<AnalyticsChunkRow> rows, Mono<AnalyticsChunkTrailer> trailer) {
    return ((AnalyticsRequest) request).decode(ResponseStatus.SUCCESS, header, rows, trailer);
  }

  @Override
  public Optional<CouchbaseException> error(AnalyticsChunkTrailer trailer, int httpStatus, RequestContext requestContext) {
    return trailer.errors().map(it -> AnalyticsChunkResponseParser.errorsToThrowable(it, requestContext, HttpResponseStatus.valueOf(httpStatus)));
  }

  @Override
  public Optional<RetryReason> retryReason(CouchbaseException error) {
    return AnalyticsMessageHandler.retryReason(error);
  }


  /**
   * Parses one response body. Holds the state of that parse, so each response gets a new instance.
   * <p>
   * Like the Netty-based parser, the header is complete when the first row, the status,
   * or the metrics arrive.
   */
  private static final class Parser {
    private final Consumer<AnalyticsChunkHeader> headerCallback;
    private final Consumer<byte[]> rowCallback;

    private boolean headerCompleted;
    private @Nullable String requestId;
    private byte @Nullable [] signature;
    private @Nullable String clientContextId;
    private @Nullable String status;
    private byte @Nullable [] metrics;
    private byte @Nullable [] warnings;
    private byte @Nullable [] errors;
    private byte @Nullable [] plans;

    private Parser(Consumer<AnalyticsChunkHeader> headerCallback, Consumer<byte[]> rowCallback) {
      this.headerCallback = requireNonNull(headerCallback);
      this.rowCallback = requireNonNull(rowCallback);
    }

    private AnalyticsChunkTrailer parse(InputStream is) {
      try (JsonStreamParser parser = newStreamParser()) {
        parser.feed(is);
        parser.endOfInput();
      }
      return new AnalyticsChunkTrailer(
        status,
        metrics,
        Optional.ofNullable(warnings),
        Optional.ofNullable(errors),
        Optional.ofNullable(plans)
      );
    }

    private JsonStreamParser newStreamParser() {
      return JsonStreamParser.builder()
        .doOnValue("/requestID", v -> requestId = v.readString())
        .doOnValue("/signature", v -> signature = v.bytes())
        .doOnValue("/plans", v -> plans = v.bytes())
        .doOnValue("/clientContextID", v -> clientContextId = v.readString())
        .doOnValue("/results/-", v -> {
          maybeCompleteHeader();
          rowCallback.accept(v.bytes());
        })
        .doOnValue("/status", v -> {
          maybeCompleteHeader();
          status = v.readString();
        })
        .doOnValue("/metrics", v -> {
          maybeCompleteHeader();
          metrics = v.bytes();
        })
        .doOnValue("/errors", v -> errors = v.bytes())
        .doOnValue("/warnings", v -> warnings = v.bytes())
        .build();
    }

    private void maybeCompleteHeader() {
      if (headerCompleted) return;
      headerCompleted = true;
      headerCallback.accept(new AnalyticsChunkHeader(
        requestId,
        Optional.ofNullable(clientContextId),
        Optional.ofNullable(signature)
      ));
    }
  }
}
