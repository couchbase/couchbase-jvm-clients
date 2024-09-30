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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.FutureCallback;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.util.HostAndPort;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownBlocking;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;
import static com.couchbase.client.core.util.ProtostellarUtil.activateSpan;

/**
 * Used to generically handle the core functionality of sending a GRPC request over Protostellar and handling the response.
 * <p>
 * Can handle any single-request-single-response setup, e.g. KV, collection management, etc.
 * <p>
 * Does not handle streaming.  {@see CoreProtostellarAccessorsStreaming} for that.
 */
public class CoreProtostellarAccessors {

  /**
   * Convenience overload that uses the default exception handling.
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  TSdkResult blocking(CoreProtostellar core,
                      ProtostellarRequest<TGrpcRequest>     request,
                      Function<ProtostellarEndpoint, TGrpcResponse> executeBlockingGrpcCall,
                      Function<TGrpcResponse, TSdkResult>   convertResponse) {
    return blocking(core, request, executeBlockingGrpcCall, convertResponse, (err) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, err));
  }

  /**
   * @param <TSdkResult> e.g. MutationResult
   * @param <TGrpcResponse> e.g. com.couchbase.client.protostellar.kv.v1.InsertResponse
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  TSdkResult blocking(CoreProtostellar core,
                      ProtostellarRequest<TGrpcRequest>     request,
                      Function<ProtostellarEndpoint, TGrpcResponse> executeBlockingGrpcCall,
                      Function<TGrpcResponse, TSdkResult>   convertResponse,
                      Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    while (true) {
      handleShutdownBlocking(core, request);
      ProtostellarEndpoint endpoint = core.endpoint();
      long start = System.nanoTime();
      RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);

      AutoCloseable scope = activateSpan(Optional.empty(), dispatchSpan, core.context().coreResources().requestTracer());

      try {
        // Make the Protostellar call.
        TGrpcResponse response = executeBlockingGrpcCall.apply(endpoint);

        request.dispatchDuration(System.nanoTime() - start);
        handleDispatchSpan(null, dispatchSpan, scope);
        TSdkResult result = convertResponse.apply(response);
        request.raisedResponseToUser(null);
        return result;
      } catch (Throwable t) {
        request.dispatchDuration(System.nanoTime() - start);
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan, scope);
        if (behaviour.retryDuration() != null) {
          try {
            Thread.sleep(behaviour.retryDuration().toMillis());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          // Loop round again for a retry.
        } else {
          request.raisedResponseToUser(behaviour.exception());
          throw behaviour.exception();
        }
      }
    }
  }

  /**
   * Convenience overload that uses the default exception handling.
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  CoreAsyncResponse<TSdkResult> async(CoreProtostellar core,
                                      ProtostellarRequest<TGrpcRequest>         request,
                                      Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                                      Function<TGrpcResponse, TSdkResult>       convertResponse) {
    return async(core, request, executeFutureGrpcCall, convertResponse, (err) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, err));
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  CoreAsyncResponse<TSdkResult> async(CoreProtostellar core,
                                      ProtostellarRequest<TGrpcRequest>         request,
                                      Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                                      Function<TGrpcResponse, TSdkResult>       convertResponse,
                                      Function<Throwable, ProtostellarRequestBehaviour>     convertException) {

    CompletableFuture<TSdkResult> ret = new CompletableFuture<>();
    CoreAsyncResponse<TSdkResult> response = new CoreAsyncResponse<>(ret, () -> {});
    asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
    return response;
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  void asyncInternal(CompletableFuture<TSdkResult> ret,
                     CoreProtostellar core,
                    ProtostellarRequest<TGrpcRequest>         request,
                    Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                    Function<TGrpcResponse, TSdkResult>       convertResponse,
                    Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
    if (handleShutdownAsync(core, ret, request)) {
      return;
    }
    ProtostellarEndpoint endpoint = core.endpoint();
    RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);
    long start = System.nanoTime();

    AutoCloseable scope = activateSpan(Optional.empty(), dispatchSpan, core.context().coreResources().requestTracer());

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.apply(endpoint);

    Futures.addCallback(response, new FutureCallback<TGrpcResponse>() {
      @Override
      public void onSuccess(TGrpcResponse response) {
        request.dispatchDuration(System.nanoTime() - start);
        handleDispatchSpan(null, dispatchSpan, scope);

        TSdkResult result = convertResponse.apply(response);

        if (request.completed()) {
          core.context().environment().orphanReporter().report(new ProtostellarBaseRequest(core, request));
        }
        else {
          request.raisedResponseToUser(null);
          ret.complete(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        request.dispatchDuration(System.nanoTime() - start);
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan, scope);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            if (!request.completed()) {
              // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
              request.raisedResponseToUser(err);
              ret.completeExceptionally(err);
            }
          }
        }
        else {
          if (!request.completed()) {
            // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
            request.raisedResponseToUser(behaviour.exception());
            ret.completeExceptionally(behaviour.exception());
          }
        }
      }
    }, core.context().environment().executor());
  }

  /**
   * Convenience overload that uses the default exception handling.
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  Mono<TSdkResult> reactive(CoreProtostellar core,
                            ProtostellarRequest<TGrpcRequest>         request,
                            Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                            Function<TGrpcResponse, TSdkResult>       convertResponse) {
    return Mono.defer(() -> {
      Sinks.One<TSdkResult> ret = Sinks.one();
      reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, (err) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, err));
      return ret.asMono();
    });
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  Mono<TSdkResult> reactive(CoreProtostellar core,
                            ProtostellarRequest<TGrpcRequest>         request,
                            Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                            Function<TGrpcResponse, TSdkResult>       convertResponse,
                            Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
    return Mono.defer(() -> {
      Sinks.One<TSdkResult> ret = Sinks.one();
      reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
      return ret.asMono();
    });
  }

  /**
   * This method must always be called at Reactive runtime, not build-time (e.g., inside a Mono.defer or similar).
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  void reactiveInternal(Sinks.One<TSdkResult> ret,
                        CoreProtostellar core,
                        ProtostellarRequest<TGrpcRequest>         request,
                        Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                        Function<TGrpcResponse, TSdkResult>       convertResponse,
                        Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
    if (handleShutdownReactive(ret, core, request)) {
      return;
    }

    ProtostellarEndpoint endpoint = core.endpoint();
    RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);
    long start = System.nanoTime();

    AutoCloseable scope = activateSpan(Optional.empty(), dispatchSpan, core.context().coreResources().requestTracer());

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.apply(endpoint);

    Futures.addCallback(response, new FutureCallback<TGrpcResponse>() {
      @Override
      public void onSuccess(TGrpcResponse response) {
        if (request.completed()) {
          core.context().environment().orphanReporter().report(new ProtostellarBaseRequest(core, request));
        }
        else {
          request.dispatchDuration(System.nanoTime() - start);
          handleDispatchSpan(null, dispatchSpan, scope);
          TSdkResult result = convertResponse.apply(response);
          request.raisedResponseToUser(null);
          ret.tryEmitValue(result).orThrow();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        request.dispatchDuration(System.nanoTime() - start);
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan, scope);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            if (!request.completed()) {
              // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
              request.raisedResponseToUser(err);
              ret.tryEmitError(err).orThrow();
            }
          }
        }
        else {
          if (!request.completed()) {
            // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
            request.raisedResponseToUser(behaviour.exception());
            ret.tryEmitError(behaviour.exception()).orThrow();
          }
        }
      }
    }, core.context().environment().executor());
  }

  private static void handleDispatchSpan(@Nullable ProtostellarRequestBehaviour behaviour, @Nullable RequestSpan dispatchSpan, @Nullable AutoCloseable scope) {
    if (dispatchSpan != null) {
      if (behaviour != null) {
        dispatchSpan.status(RequestSpan.StatusCode.ERROR);
        if (behaviour.exception() != null) {
          dispatchSpan.recordException(behaviour.exception());
        }
      }
      dispatchSpan.end();
    }
    // Note that closing the scope doesn't end the span it owns, it just removes it from ThreadLocalStorage
    if (scope != null) {
      try {
        scope.close();
      } catch (Exception e) {
        // Silently swallow - OTel should never throw anyway
      }
    }
  }

  private static <TGrpcRequest> @Nullable RequestSpan createDispatchSpan(CoreProtostellar core,
                                                                         ProtostellarRequest<TGrpcRequest> request,
                                                                         ProtostellarEndpoint endpoint) {
    RequestTracer tracer = core.context().coreResources().requestTracer();
    RequestSpan dispatchSpan;
    if (!CbTracing.isInternalTracer(tracer)) {
      dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.span());
      HostAndPort remote = endpoint.hostAndPort();
      TracingUtils.setCommonDispatchSpanAttributes(dispatchSpan, null, null, 0, remote.host(), remote.port(), null);
    } else {
      dispatchSpan = null;
    }
    return dispatchSpan;
  }
}
