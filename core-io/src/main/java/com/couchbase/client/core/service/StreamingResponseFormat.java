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
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryReason;
import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The service-specific parts of a streamed JSON response (like a query or search result),
 * for {@link StreamingJsonResponseHandler}.
 * <p>
 * Such a response has three parts: a header (fields that come before the rows), the rows,
 * and a trailer (fields that come after the rows, like status and metrics).
 *
 * @param <H> header
 * @param <ROW> row
 * @param <T> trailer
 * @param <R> the response the request completes with
 */
@NullMarked
interface StreamingResponseFormat<H, ROW, T, R extends Response> {

  /**
   * Names the service in messages. For example, "query" in "Failed to process query response".
   */
  String serviceName();

  /**
   * Parses the response body.
   * <p>
   * Calls {@code onHeader} once, as soon as the header is complete (for example, when the first
   * row or the status arrives), then {@code onRow} for each row, then returns the trailer.
   * The callbacks run on the calling thread, in the middle of parsing, and may block.
   *
   * @throws RuntimeException if the body can't be read or parsed
   */
  T parse(InputStream body, Consumer<H> onHeader, Consumer<byte[]> onRow);

  ROW newRow(byte[] row);

  /**
   * Returns the response the request completes with (using the request's own {@code decode} method).
   */
  R newResponse(Request<R> request, H header, Flux<ROW> rows, Mono<T> trailer);

  /**
   * Returns the error reported by the response body, if any.
   * <p>
   * For an error response, this is why the request failed (or should be retried; see {@link #retryReason}).
   * For a response whose HTTP status says it succeeded, it's an error that happened after the request
   * completed (for example, a query error that happened after some rows were sent). If present,
   * it fails the rows. The trailer still succeeds, like in the Netty implementation.
   */
  Optional<CouchbaseException> error(T trailer, int httpStatus, RequestContext requestContext);

  /**
   * Returns the reason to retry a request whose error response reported the given error
   * (from {@link #error}), or empty if the request should fail.
   */
  Optional<RetryReason> retryReason(CouchbaseException error);
}
