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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.BaseHttpRequest;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.CbStrings;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.SingleStateful;
import okhttp3.Call;
import okhttp3.EventListener;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The parts of an OkHttp-based service that don't depend on which service it is.
 * <p>
 * Handles, for every request sent to the service's node:
 * <ul>
 *   <li>Admission: the circuit breaker, and the limit on requests in flight to the node.
 *       Requests that aren't admitted are retried (possibly on a different node).
 *   <li>Dispatch: tracing, dispatch latency, connection tracking for diagnostics, cancellation,
 *       and failures to connect or to get a response (retried, or failed if fatal).
 *   <li>{@link CoreHttpRequest}s (for example, index management and pings), apart from turning
 *       error responses into exceptions, which is service-specific ({@link #translateCoreHttpError}).
 * </ul>
 * Subclasses handle their own request types ({@link #supports}, {@link #dispatchServiceRequest}),
 * using {@link #execute} to send them.
 * <p>
 * When a subclass parses a response body, it should report a body it can't make sense of
 * (for example, one that isn't JSON) the same way as every other service: by failing the request
 * with an exception that includes the start of the body. For successful (2xx) responses, the body
 * is included only if it isn't JSON. {@link StreamingJsonResponseHandler} does that for streamed
 * results; for other responses, wrap the body with {@link HeadInterceptInputStream} to capture its start.
 * <p>
 * Unlike the Netty endpoints, the OkHttp services don't report orphaned responses (responses to requests that
 * already completed, for example because they timed out). Cancelling a request cancels its OkHttp call, which
 * closes the connection, so a late response never arrives.
 */
@NullMarked
abstract class AbstractOkHttpService implements Service {
  protected static final MediaType APPLICATION_JSON = requireNonNull(MediaType.parse("application/json"));

  /**
   * Named after the subclass, so log messages say which service they're about.
   */
  protected final Logger log = LoggerFactory.getLogger(getClass());
  private static final Logger brokenResponseLog = LoggerFactory.getLogger(AbstractOkHttpService.class);

  private final ServiceType serviceType;
  private final HostAndPort address;
  private final ServiceContext serviceContext;
  private final SingleStateful<ServiceState> state = SingleStateful.fromInitial(ServiceState.CONNECTED);
  private final AtomicBoolean disconnected = new AtomicBoolean();
  private final HttpUrl baseUrl;

  private final AttemptCircuitBreaker circuitBreaker;
  private final CircuitBreaker.CompletionCallback circuitBreakerCallback;

  /**
   * Limits the number of HTTP exchanges in flight to this node.
   * <p>
   * When the limit is reached, requests are retried (and possibly dispatched to a different node)
   * instead of waiting in OkHttp's dispatcher queue.
   */
  private final Semaphore inFlightSlots;

  /**
   * Tracks connections to this node, for diagnostics.
   */
  private final NodeConnectionTracker connectionTracker;

  /**
   * Handles the HTTP response to a request sent with {@link #execute}.
   */
  @FunctionalInterface
  protected interface ResponseHandler {
    /**
     * Called with the response (the status and headers have arrived; the body may still be arriving).
     * Must close the response body, and complete or retry the request. Should not throw.
     */
    void handle(okhttp3.Response response) throws IOException;
  }

  /**
   * @param maxInFlight the maximum number of HTTP exchanges in flight to this node at once.
   */
  protected AbstractOkHttpService(
    ServiceType serviceType,
    CoreContext context,
    HostAndPort address,
    int maxInFlight,
    CircuitBreakerConfig circuitBreakerConfig
  ) {
    this.serviceType = requireNonNull(serviceType);
    this.address = requireNonNull(address);
    this.serviceContext = new ServiceContext(context, address, serviceType, Optional.empty());

    this.baseUrl = new HttpUrl.Builder()
      .scheme(context.environment().securityConfig().tlsEnabled() ? "https" : "http")
      .host(address.host())
      .port(address.port())
      .build();

    this.circuitBreaker = AttemptCircuitBreaker.from(circuitBreakerConfig, serviceType.id() + " service on " + address);
    this.circuitBreakerCallback = circuitBreakerConfig.completionCallback();
    this.inFlightSlots = new Semaphore(maxInFlight);
    this.connectionTracker = new NodeConnectionTracker(
      serviceType,
      address,
      serviceContext.bucket().orElse(null),
      circuitBreaker::state,
      context
    );
  }

  // ---- For subclasses ----

  /**
   * Returns true if this service handles the given type of request,
   * in addition to {@link CoreHttpRequest}, which all services handle.
   */
  protected abstract boolean supports(Request<?> request);

  /**
   * Sends a request of a type this service {@link #supports}, typically using {@link #execute}.
   * The request has been admitted; the attempt must be {@link DispatchAttempt#finish finished}
   * when the HTTP exchange is done ({@link #execute} takes care of that).
   */
  protected abstract void dispatchServiceRequest(Request<?> request, DispatchAttempt attempt);

  /**
   * Returns the exception to fail a {@link CoreHttpRequest} with, when the service responds
   * with an error status. (Not called if the request bypasses exception translation.)
   */
  protected abstract Exception translateCoreHttpError(int httpStatus, String responseBody, CoreHttpRequest request);

  /**
   * Returns the URL of the service on this node, with no path.
   */
  protected final HttpUrl baseUrl() {
    return baseUrl;
  }

  /**
   * Returns the URL for the given path (and query string, if any) on this node.
   *
   * @param pathAndQuery already URL-encoded, with or without a leading slash
   */
  protected final String url(String pathAndQuery) {
    return baseUrl + CbStrings.removeStart(pathAndQuery, "/");
  }

  /**
   * Sends a request whose result is streamed JSON (like a query), and handles the response
   * with {@link StreamingJsonResponseHandler}: completes the request when the header arrives,
   * or fails or retries it if the server reports an error.
   */
  protected final <R extends Response> void executeStreaming(
    BaseHttpRequest<R> request,
    okhttp3.Request.Builder requestBuilder,
    DispatchAttempt attempt,
    StreamingResponseFormat<?, ?, ?, R> format
  ) {
    execute(request, requestBuilder, attempt, response ->
      StreamingJsonResponseHandler.handle(format, request, response, attempt, context())
    );
  }

  /**
   * Sends the request, and passes the response to the handler.
   * <p>
   * Takes care of tracing, dispatch latency, connection tracking, and finishing the attempt.
   * If the call fails (for example, can't connect), retries the request or fails it.
   * If the request is cancelled, cancels the call.
   *
   * @return the call, which was already enqueued, so the handler might be running (or done) by now.
   */
  protected final Call execute(
    BaseHttpRequest<?> request,
    okhttp3.Request.Builder requestBuilder,
    DispatchAttempt attempt,
    ResponseHandler handler
  ) {
    DispatchTimer dispatchTimer = new DispatchTimer(request);

    Call call = enqueue(request, requestBuilder, dispatchTimer, new okhttp3.Callback() {
      @Override
      public void onResponse(Call call, okhttp3.Response response) throws IOException {
        try {
          try {
            NanoTimestamp dispatchStart = dispatchTimer.start;
            if (dispatchStart != null) request.context().dispatchLatency(dispatchStart.elapsedNanos());
            RequestSpan dispatchSpan = dispatchTimer.span;
            if (dispatchSpan != null) dispatchSpan.end();
          } catch (RuntimeException e) {
            // For example, a custom tracer that throws. Don't let it stop the response from being handled,
            // or the request would never complete, and the response body would never be closed.
            log.warn("Failed to record dispatch latency or end the dispatch span.", e);
          }

          handler.handle(response);

        } finally {
          attempt.finish();
        }
      }

      @Override
      public void onFailure(Call call, IOException e) {
        try {
          recordCallFailure(attempt, call, e);
        } finally {
          attempt.finish();
          commonOnFailure(request, dispatchTimer.span, call, e);
        }
      }
    });

    request.setCancellationHook(call::cancel);
    return call;
  }

  // ---- Service ----

  @Override
  public <R extends Request<? extends Response>> void send(R request) {
    if (!(request instanceof CoreHttpRequest || supports(request))) {
      throw new IllegalArgumentException("Unsupported request type: " + request);
    }

    // Like the Netty endpoints: don't send a request whose deadline has passed (for example, during a retry
    // backoff) but whose timeout hasn't fired yet. The server would do the work, and for a non-idempotent request,
    // could make a change after the caller has been told the request timed out.
    if (request.timeoutElapsed()) {
      request.cancel(CancellationReason.TIMEOUT);
    }
    if (request.completed()) {
      return;
    }

    AttemptCircuitBreaker.Permit permit = circuitBreaker.tryAcquire();
    if (permit == null) {
      RetryOrchestrator.maybeRetry(context(), request, RetryReason.ENDPOINT_CIRCUIT_OPEN);
      return;
    }

    // Enforce the per-node connection limit, and retry (possibly on another node) if the limit is exceeded.
    if (!inFlightSlots.tryAcquire()) {
      permit.release();
      RetryOrchestrator.maybeRetry(context(), request, RetryReason.ENDPOINT_NOT_AVAILABLE);
      return;
    }

    DispatchAttempt attempt = new DispatchAttempt(request, permit, circuitBreakerCallback, inFlightSlots, address);

    try {
      if (request instanceof CoreHttpRequest) {
        sendCoreHttpRequest((CoreHttpRequest) request, attempt);
      } else {
        dispatchServiceRequest(request, attempt);
      }

    } catch (Throwable t) {
      // The call was never enqueued, so no callback will finish the attempt.
      attempt.finish();
      throw t;
    }
  }

  @Override
  public void connect() {
  }

  /**
   * Reports the service as disconnected, and completes the {@link #states()} stream, like
   * {@code PooledService}, so anything waiting for the service to reach a state learns it never will.
   * <p>
   * Doesn't cancel calls in flight; they use the shared OkHttp client, and finish on their own.
   * Their connections go back to the shared pool, which closes them once they're idle.
   */
  @Override
  public void disconnect() {
    if (disconnected.compareAndSet(false, true)) {
      state.transition(ServiceState.DISCONNECTED);
      state.close();
    }
  }

  @Override
  public ServiceContext context() {
    return serviceContext;
  }

  @Override
  public ServiceType type() {
    return serviceType;
  }

  @Override
  public Stream<EndpointDiagnostics> diagnostics() {
    return connectionTracker.diagnostics().stream();
  }

  @Override
  public Stream<InternalEndpointDiagnostics> internalDiagnostics() {
    return connectionTracker.internalDiagnostics().stream();
  }

  @Override
  public ServiceState state() {
    return state.state();
  }

  @Override
  public Flux<ServiceState> states() {
    return this.state.states();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
      "address=" + address +
      '}';
  }

  // ---- Internals ----

  /**
   * Call once the call has a connection: the local attributes come from the details
   * the connection tracker records in the request context when the call acquires its connection.
   */
  private @Nullable RequestSpan newDispatchSpan(Request<?> request) {
    if (request.requestSpan() == null) return null;

    RequestTracer tracer = context().coreResources().requestTracer();
    RequestSpan dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.requestSpan());

    if (!CbTracing.isInternalTracer(tracer)) {
      RequestContext requestContext = request.context();
      HostAndPort local = requestContext.lastDispatchedFrom();
      TracingDecorator tip = context().coreResources().tracingDecorator();
      tip.provideCommonDispatchSpanAttributes(
        dispatchSpan,
        requestContext.lastChannelId(),
        local == null ? null : local.host(),
        local == null ? 0 : local.port(),
        requestContext.lastDispatchedToNode().canonical().host(),
        address.port(), // Like Netty: the service's port, not the canonical (manager) port.
        address.host(),
        address.port(),
        request.operationId()
      );
    }

    return dispatchSpan;
  }

  /**
   * Records the circuit breaker outcome of a failed call.
   * Failures that happened before the request was sent (while connecting)
   * are judged differently from failures after the node received the request.
   */
  private static void recordCallFailure(DispatchAttempt attempt, Call call, IOException e) {
    if (CouchbaseOkHttpClient.requestStarted(call.request())) {
      attempt.recordFailure(e);
    } else {
      attempt.recordConnectFailure(e);
    }
  }

  private void commonOnFailure(
    Request<?> request,
    @Nullable RequestSpan dispatchSpan,
    Call call,
    IOException e
  ) {
    if (dispatchSpan != null) {
      dispatchSpan.recordException(e);
      dispatchSpan.status(RequestSpan.StatusCode.ERROR);
      dispatchSpan.end();
    }

    boolean requestStarted = CouchbaseOkHttpClient.requestStarted(call.request());
    log.debug("Sending request to {} failed; requestStarted={}", call.request().url(), requestStarted, e);

    String tlsHint = OkHttpTlsHelper.permanentTlsFailureHint(e);
    if (tlsHint != null) {
      // For example, an untrusted server certificate, or a port that doesn't use TLS. Retrying won't help.
      // (Other TLS failures, like the server closing the connection during the handshake, are retried below.)
      // AuthenticationFailureException, so bucket config loading backs off (see DefaultConfigurationProvider).
      request.fail(
        new AuthenticationFailureException(
          "Failed to establish secure connection to server - " + tlsHint,
          new GenericRequestErrorContext(request),
          e
        )
      );
      return;
    }

    if (e instanceof UnknownServiceException) {
      // Thrown by OkHttp for various fatal errors
      request.fail(
        new CouchbaseException(
          "Failed to dispatch request",
          e,
          new GenericRequestErrorContext(request)
        )
      );
      return;
    }

    // Anything else (connection refused, connection reset, socket timeout, ...) might work on another attempt.
    RetryReason retryReason = requestStarted
      ? RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT
      : RetryReason.ENDPOINT_NOT_AVAILABLE;

    RetryOrchestrator.maybeRetry(context(), request, retryReason);
  }

  /**
   * Retries a request whose response broke off: the connection failed while reading the body, before the request
   * completed. Like the Netty implementation, which does this when the connection closes with a request in flight.
   * <p>
   * The request reached the server, which may have acted on it, so the retry strategy only retries it if it's
   * idempotent ({@link RetryReason#CHANNEL_CLOSED_WHILE_IN_FLIGHT}). Otherwise, it fails with a
   * {@code RequestCanceledException}, which says the outcome is unknown.
   */
  static void maybeRetryAfterBrokenResponse(CoreContext retryContext, Request<?> request, IOException readFailure) {
    brokenResponseLog.debug("Response to {} broke off: {}", request.getClass().getSimpleName(), readFailure.toString());
    RetryOrchestrator.maybeRetry(retryContext, request, RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT);
  }

  private Call enqueue(
    Request<?> request,
    okhttp3.Request.Builder requestBuilder,
    DispatchTimer dispatchTimer,
    okhttp3.Callback callback
  ) {
    Call call = context().core().okHttpClient().newCall(requestBuilder, request.context(), request.timeout());
    call.addEventListener(connectionTracker);
    call.addEventListener(dispatchTimer);
    call.enqueue(callback);
    return call;
  }

  /**
   * Starts the dispatch latency timer and the dispatch span when OkHttp starts writing the request.
   * <p>
   * Not when the call is enqueued, because OkHttp connects lazily: the time before the request is written
   * can include connecting and the TLS handshake, which aren't part of dispatching the request.
   * Starting here also matches the Netty endpoints, which start timing when the request is written
   * to a channel that's already connected. If the call fails before the request is written (for example,
   * can't connect), there's no dispatch span, as with Netty.
   * <p>
   * OkHttp calls this on the thread that runs the call, which is also the thread that runs the call's callback.
   */
  private final class DispatchTimer extends EventListener {
    private final Request<?> request;

    volatile @Nullable NanoTimestamp start;
    volatile @Nullable RequestSpan span;

    DispatchTimer(Request<?> request) {
      this.request = requireNonNull(request);
    }

    @Override
    public void requestHeadersStart(Call call) {
      if (start != null) {
        return; // Doesn't happen, since the client doesn't retry or follow redirects. But don't leak a span if it does.
      }
      start = NanoTimestamp.now();
      try {
        span = newDispatchSpan(request);
      } catch (RuntimeException e) {
        // For example, a custom tracer that throws. Don't let it fail the call.
        log.warn("Failed to start the dispatch span.", e);
      }
    }
  }

  /**
   * Returns the ID of the connection the request was dispatched on, in the format
   * {@link CoreHttpResponse#channelId()} expects (no "0x" prefix), or null if unknown.
   * The connection tracker records it when the call acquires a connection.
   */
  private static @Nullable String channelIdForResponse(RequestContext context) {
    String id = context.lastChannelId();
    return id == null ? null : CbStrings.removeStart(id, "0x");
  }

  private void sendCoreHttpRequest(CoreHttpRequest request, DispatchAttempt attempt) {
    okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
      .url(url(request.pathAndQueryString()))
      .method(request.method(), requestBody(request.method(), request.contentAsByteArray()));
    request.forEachHeader(requestBuilder::header);

    execute(request, requestBuilder, attempt, response -> {
      try (ResponseBody responseBody = response.body()) {
        ResponseStatus status = HttpProtocol.decodeStatus(response.code());
        if (status.success() || !request.failOnErrorStatus()) {
          CoreHttpResponse coreResponse = new CoreHttpResponse(
            status,
            responseBody.bytes(),
            response.code(),
            channelIdForResponse(request.context()),
            request.context()
          );
          attempt.recordOutcome(coreResponse, null);
          request.succeed(coreResponse);

        } else {
          String body = responseBody.string();
          Exception error = request.bypassExceptionTranslation()
            ? new HttpStatusCodeException(response.code(), body, request, null)
            : translateCoreHttpError(response.code(), body, request);
          attempt.recordOutcome(null, error);
          request.fail(error);
        }

      } catch (IOException e) {
        // Reading the body failed: the connection broke, not the content.
        attempt.recordFailure(e);
        maybeRetryAfterBrokenResponse(context(), request, e);

      } catch (Throwable t) {
        attempt.recordFailure(t);
        request.fail(t);
      }
    });
  }

  /**
   * Returns the body for a request, or null for none. OkHttp requires a body for POST, PUT, and PATCH requests,
   * even if it's empty (for example, deploying an eventing function, or flushing a bucket), and forbids one for
   * GET and HEAD requests.
   */
  static @Nullable RequestBody requestBody(String method, byte[] content) {
    boolean bodyRequired = method.equals("POST") || method.equals("PUT") || method.equals("PATCH");
    return content.length == 0 && !bodyRequired ? null : RequestBody.create(content);
  }
}
