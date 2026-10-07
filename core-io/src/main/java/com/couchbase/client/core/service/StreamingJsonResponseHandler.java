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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import okhttp3.ResponseBody;
import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Handles the response to a request whose result is streamed JSON, like a query or search result.
 * The service-specific parts are supplied by a {@link StreamingResponseFormat}.
 * <p>
 * For a successful (2xx) response, completes the request as soon as the header has arrived, with a response whose rows and trailer
 * arrive later, as the body is parsed. Rows are passed to the subscriber with backpressure: if
 * the subscriber falls behind, parsing (and reading from the connection) pauses.
 * <p>
 * If the body isn't JSON (for example, an HTML page from a proxy), the request fails with an exception
 * whose message includes the start of the body (the first {@value #BODY_HEAD_LIMIT} bytes, redacted
 * as user data). Other parse failures are reported without the body.
 * <p>
 * For an error response, fails the request with the error reported in the body, or retries it
 * if the error is retryable. If the body has no error, or isn't JSON, the request fails with an
 * exception whose message includes the start of the body.
 * <p>
 * Runs on the thread that received the response, until the whole body has been parsed.
 */
@NullMarked
final class StreamingJsonResponseHandler {

  /**
   * How many rows can be buffered before parsing pauses for the subscriber to catch up.
   */
  private static final int ROW_BUFFER_HIGH_WATER_MARK = 64;

  /**
   * How many bytes from the start of an unexpected response body to report.
   */
  static final int BODY_HEAD_LIMIT = 1024;

  private StreamingJsonResponseHandler() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Completes, fails, or retries the request, depending on the response.
   *
   * @param streamingReadTimeout socket read timeout for the rest of a successful response,
   * once the request has completed (see the comment where it's applied).
   * @param retryContext context for retrying the request
   */
  static <H, ROW, T, R extends Response> void handle(
    StreamingResponseFormat<H, ROW, T, R> format,
    Request<R> request,
    okhttp3.Response httpResponse,
    DispatchAttempt attempt,
    Duration streamingReadTimeout,
    CoreContext retryContext
  ) {
    if (HttpProtocol.decodeStatus(httpResponse.code()).success()) {
      handleSuccess(format, request, httpResponse, attempt, streamingReadTimeout);
    } else {
      handleFailure(format, request, httpResponse, attempt, retryContext);
    }
  }

  private static <H, ROW, T, R extends Response> void handleSuccess(
    StreamingResponseFormat<H, ROW, T, R> format,
    Request<R> request,
    okhttp3.Response httpResponse,
    DispatchAttempt attempt,
    Duration streamingReadTimeout
  ) {
    Sinks.One<T> trailerSink = Sinks.one();
    BlockingStreamBridge<ROW> rowBridge = new BlockingStreamBridge<>(ROW_BUFFER_HIGH_WATER_MARK);
    AtomicBoolean headerArrived = new AtomicBoolean();

    try (
      ResponseBody responseBody = httpResponse.body();
      // Remember the start of the body in case we need to report an unexpected server response.
      HeadInterceptInputStream bodyStream = new HeadInterceptInputStream(responseBody.byteStream(), BODY_HEAD_LIMIT)
    ) {
      T trailer;
      try {
        trailer = format.parse(
          bodyStream,
          header -> {
            headerArrived.set(true);
            R r = format.newResponse(request, header, rowBridge.rows(), trailerSink.asMono());
            request.succeed(r);
            attempt.recordOutcome(r, null);

            // Desired behavior for compatibility with the previous implementation:
            // The SDK stops enforcing the request timeout as soon as it completes the response future,
            // which happens when the header arrives (for example, with the first result row).
            // After that, the request timeout no longer applies.
            //
            // Until now, the socket read timeout has been the request timeout (plus a grace period),
            // because it can take that long for the header to arrive. Now that the SDK has stopped
            // enforcing the request timeout, dial down the read timeout to something that defends against
            // dead connections, without limiting how long it takes to stream the whole result.
            // The timeout applies to each read, so the remaining rows can take as long as they need,
            // as long as the server keeps sending data when we ask for it.
            CouchbaseOkHttpClient.setSocketReadTimeout(httpResponse.request(), streamingReadTimeout);
          },
          row -> {
            try {
              rowBridge.emitNext(format.newRow(row));

            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("Interrupted while emitting row", e);
            }
          }
        );
      } catch (RuntimeException parseFailure) {
        // Handled here, while the body stream is still open, so the start of the body can be reported.
        attempt.recordFailure(parseFailure);

        DecodingFailureException failure = looksLikeJson(bodyStream)
          ? new DecodingFailureException("Failed to process " + format.serviceName() + " response", parseFailure)
          // Not JSON at all (for example, an HTML page from a proxy). Report the start of the body.
          : unparseable(httpResponse.code(), bodyStream, parseFailure);

        rowBridge.fail(failure);
        trailerSink.tryEmitError(failure);
        request.fail(failure); // no effect if the request already succeeded (when the header was complete)
        return;
      }

      // A "successful" response can still report an error.
      CouchbaseException e = format.error(trailer, httpResponse.code(), request.context()).orElse(null);

      if (!headerArrived.get()) {
        // A well-behaved server doesn't do this, but don't leave the request waiting for its timeout.
        // Report the error the response described, if any; otherwise, that the response was incomplete.
        // (The body is JSON, so it isn't included in the message, like other 2xx responses we can't process.)
        CouchbaseException failure = e != null
          ? e
          : new DecodingFailureException("Failed to process " + format.serviceName() + " response:"
          + " it ended before any results or status arrived.");
        attempt.recordOutcome(null, failure);
        request.fail(failure);
        return;
      }

      if (e != null) {
        rowBridge.fail(e);
      } else {
        rowBridge.complete();
      }

      // Like the Netty implementation, the trailer succeeds even if the response reported an error,
      // so callers can still get the metadata (which may describe the error).
      trailerSink.tryEmitValue(trailer);

    } catch (Exception e) {
      attempt.recordFailure(e);

      RuntimeException decodingFailure = new DecodingFailureException("Failed to process " + format.serviceName() + " response", e);
      rowBridge.fail(decodingFailure);
      trailerSink.tryEmitError(decodingFailure);

      // in case the failure happened prior to header completion
      request.fail(decodingFailure);
    }
  }

  /**
   * Fails the response future or retries the request.
   * <ul>
   *   <li>If the response body is not valid JSON, we fail with a DecodingFailureException
   *       whose message includes the start of the body.
   *
   *   <li>If it reports an error, we either fail the request with an exception derived from
   *       the error, or retry the request if the error is retryable.
   *
   *   <li>If the response body is valid JSON but reports no error, we fail the response future
   *       with a CouchbaseException whose message includes the HTTP status code and response body.
   * </ul>
   */
  private static <H, ROW, T, R extends Response> void handleFailure(
    StreamingResponseFormat<H, ROW, T, R> format,
    Request<R> request,
    okhttp3.Response httpResponse,
    DispatchAttempt attempt,
    CoreContext retryContext
  ) {
    try (
      ResponseBody responseBody = httpResponse.body();
      // Remember some of the body in case we need to report an unexpected server response.
      HeadInterceptInputStream bodyStream = new HeadInterceptInputStream(responseBody.byteStream(), BODY_HEAD_LIMIT)
    ) {
      T trailer;
      try {
        trailer = format.parse(
          bodyStream,
          header -> {
            // Ignore the header; all we care about is the error.
          },
          row -> {
            // We don't expect a failed response to have rows. If rows are somehow present, ignore them.
          }
        );
      } catch (RuntimeException parseFailure) {
        // Not JSON (for example, an error page from a proxy). Report the start of the body.
        attempt.recordFailure(parseFailure);
        request.fail(unparseable(httpResponse.code(), bodyStream, parseFailure));
        return;
      }

      CouchbaseException e = format.error(trailer, httpResponse.code(), request.context()).orElse(null);

      if (e != null) {
        // The node responded, even if the request is about to be retried.
        attempt.recordOutcome(null, e);

        Optional<RetryReason> retryReason = format.retryReason(e);
        if (retryReason.isPresent()) {
          RetryOrchestrator.maybeRetry(retryContext, request, retryReason.get());
        } else {
          request.fail(e);
        }
        return;
      }

      CouchbaseException unexpected = unexpected(httpResponse.code(), bodyStream);
      attempt.recordOutcome(null, unexpected);
      request.fail(unexpected);

    } catch (Exception e) {
      attempt.recordFailure(e);
      request.fail(new DecodingFailureException(e));
    }
  }

  // ---- Reporting unexpected response bodies ----
  //
  // When a body can't be parsed (for example, it isn't JSON, like an error page from a proxy), or doesn't
  // contain what the service expected, the request fails with an exception whose message includes the
  // start of the body, which is usually more helpful than a parse error. These must be called before
  // the body stream is closed, because they may read the rest of the head from it.

  /**
   * Returns true if the body appears to be a JSON object or array, judging by its first
   * non-whitespace character. Doesn't check whether the rest of the body is valid JSON.
   * <p>
   * Reads the rest of the head from the stream if needed (best effort).
   */
  static boolean looksLikeJson(HeadInterceptInputStream body) {
    fillHead(body);
    for (byte b : body.getHead()) {
      switch (b) {
        case ' ':
        case '\t':
        case '\n':
        case '\r':
          continue;
        case '{':
        case '[':
          return true;
        default:
          return false;
      }
    }
    return false;
  }

  /**
   * Returns the exception for a response body that couldn't be parsed.
   * <p>
   * The parser may have given up before reading much of the body, so this first reads the rest
   * of the head from the stream (best effort, since the stream may have failed too).
   */
  private static DecodingFailureException unparseable(int httpStatus, HeadInterceptInputStream body, Throwable cause) {
    fillHead(body);
    return new DecodingFailureException(
      "Request failed. HTTP status code: " + httpStatus + ". Failed to parse response body: " + describe(body),
      cause
    );
  }

  /**
   * Returns the exception for a response body that was parsed, but didn't contain what the
   * service expected (for example, an error response without any errors in it).
   */
  private static CouchbaseException unexpected(int httpStatus, HeadInterceptInputStream body) {
    return new CouchbaseException(
      "Request failed. HTTP status code: " + httpStatus + ". Response had unexpected structure. Response body: " + describe(body)
    );
  }

  private static void fillHead(HeadInterceptInputStream body) {
    try {
      body.fillHead();
    } catch (IOException ignored) {
      // Use what we have.
    }
  }

  private static String describe(HeadInterceptInputStream body) {
    String head = body.getHeadAsString();
    return head.isEmpty() ? "<empty>" : redactUser(head).toString();
  }
}
