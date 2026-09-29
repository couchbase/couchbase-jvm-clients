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
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.CbStrings;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.SingleStateful;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownServiceException;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static com.couchbase.client.core.io.netty.HttpProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.TracingUtils.setCommonDispatchSpanAttributes;
import static com.couchbase.client.core.io.netty.query.QueryChunkResponseParser.errorsToThrowable;
import static com.couchbase.client.core.io.netty.query.QueryMessageHandler.qualifiesForRetry;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.service.CouchbaseOkHttpClient.newRequestBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@NullMarked
public class OkHttpQueryService implements Service {
  private static final Logger log = LoggerFactory.getLogger(OkHttpQueryService.class);

  private static final MediaType APPLICATION_JSON = requireNonNull(MediaType.parse("application/json"));

  private final HostAndPort address;
  private final ServiceContext serviceContext;
  private final SingleStateful<ServiceState> state = SingleStateful.fromInitial(ServiceState.CONNECTED);
  private final HttpUrl baseUrl;
  private final HttpUrl queryServiceUrl;

  public OkHttpQueryService(
    QueryServiceConfig config,
    CoreContext context,
    HostAndPort address
  ) {
    this.address = requireNonNull(address);
    this.serviceContext = new ServiceContext(context, address, ServiceType.QUERY, Optional.empty());

    this.baseUrl = new HttpUrl.Builder()
      .scheme(context.environment().securityConfig().tlsEnabled() ? "https" : "http")
      .scheme("http")
      .host(address.host())
      .port(address.port())
      .build();

    this.queryServiceUrl = baseUrl.newBuilder()
      .addPathSegment("query")
      .addPathSegment("service")
      .build();
  }

  @Override
  public void connect() {
  }

  @Override
  public void disconnect() {
  }

  @Override
  public <R extends Request<? extends Response>> void send(R request) {
    request.context().lastDispatchedTo(address);

    if (request instanceof QueryRequest) {
      sendQueryRequest((QueryRequest) request);

    } else if (request instanceof CoreHttpRequest) {
      sendCoreHttpRequest((CoreHttpRequest) request);

    } else {
      throw new IllegalArgumentException("Unsupported request type: " + request);
    }
  }

  @Nullable RequestSpan newDispatchSpan(Request<?> request) {
    if (request.requestSpan() == null) return null;

    RequestTracer tracer = context().coreResources().requestTracer();
    RequestSpan dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.requestSpan());

    if (!CbTracing.isInternalTracer(tracer)) {
      HostAndPort canonicalRemote = request.context().lastDispatchedToNode().canonical();
      TracingDecorator tip = context().coreResources().tracingDecorator();
      setCommonDispatchSpanAttributes(
        tip,
        dispatchSpan,
        // TODO local address and canonical remote address
        null, // local channel id
        null, // local host
        0, // local port
        canonicalRemote.host(),
        canonicalRemote.port(),
        address.host(),
        address.port(),
        request.operationId()
      );
    }

    return dispatchSpan;
  }


  public void sendQueryRequest(QueryRequest queryRequest) {
    RequestBody requestBody = queryRequest.idempotent()
      ? RequestBody.create(queryRequest.query(), APPLICATION_JSON)
      : OneShotRequestBody.createOneShot(queryRequest.query(), APPLICATION_JSON); // don't let OkHttp retransmit this request

    okhttp3.Request.Builder requestBuilder = newRequestBuilder()
      .url(queryServiceUrl)
      .method("POST", requestBody);

    NanoTimestamp dispatchStart = NanoTimestamp.now();

    RequestSpan dispatchSpan = newDispatchSpan(queryRequest);
    Call call = enqueue(requestBuilder, queryRequest.timeout(), new okhttp3.Callback() {
      @Override
      public void onResponse(Call call, okhttp3.Response response) throws IOException {
        queryRequest.context().dispatchLatency(dispatchStart.elapsedNanos());
        if (dispatchSpan != null) dispatchSpan.end();

        // Desired behavior for compatibility with previous implementation:
        // The SDK stops enforcing the query timeout as soon as it completes the response future,
        // which happens as soon as it receives the first result row. After that, there is no
        // timeout, and no application-level protection against dead connections.
        //
        // For the OkHttp implementation, we initially set the socket read timeout to the query timeout
        // because it can take up to that long for the first response bytes to arrive.
        // Then, when the first bytes of the response arrive (HERE!), we dial down the Okio Source's
        // read timeout to something that defends against dead connections.
        response.body().source().timeout().timeout(15, SECONDS);

        ResponseStatus responseStatus = HttpProtocol.decodeStatus(response.code());
        if (responseStatus.success()) {
          handleSuccessfulResponse(queryRequest, response);
        } else {
          handleFailedResponse(queryRequest, response);
        }
      }

      @Override
      public void onFailure(Call call, IOException e) {
        commonOnFailure(queryRequest, dispatchSpan, call, e);
      }
    });

    queryRequest.setCancellationHook(call::cancel);
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
    log.debug("Sending request to {} failed; requestStarted={}", queryServiceUrl, requestStarted, e);

    if (e instanceof SSLException) {
      // Untrusted server certificate, hostname mismatch, etc.
      request.fail(
        new AuthenticationFailureException(
          "Failed to establish secure connection to server.",
          new GenericRequestErrorContext(request),
          e
        )
      );
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
    }

    RetryReason retryReason = requestStarted
      ? RetryReason.CHANNEL_CLOSED_WHILE_IN_FLIGHT
      : RetryReason.ENDPOINT_NOT_AVAILABLE;

    RetryOrchestrator.maybeRetry(context(), request, retryReason);
  }

  private void handleSuccessfulResponse(
    QueryRequest queryRequest,
    okhttp3.Response httpResponse
  ) {
    Sinks.One<QueryChunkTrailer> trailerSink = Sinks.one();
    BlockingStreamBridge<QueryChunkRow> rowBridge = new BlockingStreamBridge<>(64);

    try (
      ResponseBody responseBody = httpResponse.body();
      InputStream bodyStream = responseBody.byteStream()
    ) {
      QueryChunkTrailer trailer = QueryResponseParser.parse(
        bodyStream,
        header -> {
          QueryResponse r = new QueryResponse(
            ResponseStatus.SUCCESS,
            header,
            rowBridge.rows(),
            trailerSink.asMono()
          );
          queryRequest.succeed(r);
        },
        row -> {
          try {
            rowBridge.emitNext(new QueryChunkRow(row));

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while emitting row", e);
          }
        }
      );

      // a "successful" response can still have errors.... maybe?
      CouchbaseException e = trailer.errors()
        .map(it -> errorsToThrowable(it, httpResponse.code(), queryRequest.context()))
        .orElse(null);

      if (e != null) {
        rowBridge.fail(e);
        trailerSink.tryEmitError(e);
      } else {
        rowBridge.complete();
        trailerSink.tryEmitValue(trailer);
      }

    } catch (Exception e) {
      RuntimeException decodingFailure = new DecodingFailureException("Failed to process query response", e);
      rowBridge.fail(decodingFailure);
      trailerSink.tryEmitError(decodingFailure);

      // in case the failure happened prior to header completion
      queryRequest.fail(decodingFailure);
    }
  }

  private static Optional<byte[]> parseErrors(InputStream bodyStream) {
    QueryChunkTrailer trailer = QueryResponseParser.parse(
      bodyStream,
      header -> {
        // Ignore the header; all we care about is the errors in the trailer.
      },
      row -> {
        // We don't expect a failed response to have rows. If rows are somehow present, ignore them.
      }
    );
    return trailer.errors();
  }

  /**
   * Fails the query response future or retries the request.
   * <ul>
   *   <li>If the response body is not valid JSON, we fail with a DecodingFailureException.
   *
   *   <li>If it has an `errors` field, we either fail it with an exception derived from
   *       the first error, or retry the request if the error is retryable.
   *
   *   <li>If the response body is valid JSON but has no `errors` field,
   *       we fail the response future with a CouchbaseException whose message includes
   *       the HTTP status code and response body.
   * </ul>
   */
  private void handleFailedResponse(
    QueryRequest queryRequest,
    okhttp3.Response httpResponse
  ) {
    try (
      ResponseBody responseBody = httpResponse.body();
      // Remember some of the body in case we need to report an unexpected server response.
      HeadInterceptInputStream bodyStream = new HeadInterceptInputStream(responseBody.byteStream(), 1024)
    ) {
      Optional<byte[]> errors = parseErrors(bodyStream);
      CouchbaseException e = errors
        .map(it -> errorsToThrowable(it, httpResponse.code(), queryRequest.context()))
        .orElse(null);

      if (e != null) {
        Optional<RetryReason> qualifies = qualifiesForRetry(e.context());
        if (qualifies.isPresent()) {
          RetryOrchestrator.maybeRetry(context(), queryRequest, qualifies.get());
        } else {
          queryRequest.fail(e);
        }
        return;
      }

      String head = bodyStream.getHeadAsString();
      String body = head.isEmpty() ? "<empty>" : redactUser(head).toString();
      queryRequest.fail(new CouchbaseException("Request failed. HTTP status code: " + httpResponse.code() + ". Response body: " + body));

    } catch (Exception e) {
      queryRequest.fail(new DecodingFailureException(e));
    }
  }


  private OkHttpClient client(Duration timeout) {
    return context().core()
      .okHttpClient()
      .clientWithTimeout(timeout);
  }

  private Call enqueue(
    okhttp3.Request.Builder requestBuilder,
    Duration timeout,
    okhttp3.Callback callback
  ) {
    OkHttpClient client = client(timeout);
    Call call = client.newCall(requestBuilder.build());
    call.enqueue(callback);
    return call;
  }

  private void sendCoreHttpRequest(CoreHttpRequest request) {
    byte[] content = request.contentAsByteArray();
    okhttp3.Request.Builder requestBuilder = newRequestBuilder()
      .url((baseUrl + CbStrings.removeStart(request.pathAndQueryString(), "/")))
      .method(
        request.method(),
        content.length == 0 ? null : RequestBody.create(content)
      );
    request.forEachHeader(requestBuilder::header);

    RequestSpan dispatchSpan = newDispatchSpan(request);

    NanoTimestamp dispatchStart = NanoTimestamp.now();
    Call call = enqueue(requestBuilder, request.timeout(), new okhttp3.Callback() {
      @Override
      public void onResponse(Call call, okhttp3.Response response) throws IOException {
        if (dispatchSpan != null) dispatchSpan.end();

        request.context().dispatchLatency(dispatchStart.elapsedNanos());

        try (ResponseBody responseBody = response.body()) {
          ResponseStatus status = decodeStatus(response.code());
          if (status.success()) {
            CoreHttpResponse coreResponse = new CoreHttpResponse(
              decodeStatus(response.code()),
              responseBody.bytes(),
              response.code(),
              request.context()
            );
            request.succeed(coreResponse);

          } else {
            String body = responseBody.string();
            Exception error = request.bypassExceptionTranslation()
              ? new HttpStatusCodeException(response.code(), body, request, null)
              : new CouchbaseException("Unknown query error: " + body); // todo other services have different logic, but this is all the legacy query endpoint does
            request.fail(error);
          }

        } catch (Throwable t) {
          request.fail(new DecodingFailureException("failed to process HTTP response", t));
        }
      }

      @Override
      public void onFailure(Call call, IOException e) {
        commonOnFailure(request, dispatchSpan, call, e);
      }
    });

    request.setCancellationHook(call::cancel);
  }

  @Override
  public ServiceContext context() {
    return serviceContext;
  }

  @Override
  public ServiceType type() {
    return ServiceType.QUERY;
  }

  @Override
  public Stream<EndpointDiagnostics> diagnostics() {
    return Stream.empty();
  }

  @Override
  public Stream<InternalEndpointDiagnostics> internalDiagnostics() {
    return Stream.empty();
  }

  @Override
  public ServiceState state() {
    return ServiceState.CONNECTED;
  }

  @Override
  public Flux<ServiceState> states() {
    return this.state.states();
  }

  @Override
  public String toString() {
    return "OkHttpQueryService{" +
      "address=" + address +
      '}';
  }
}
