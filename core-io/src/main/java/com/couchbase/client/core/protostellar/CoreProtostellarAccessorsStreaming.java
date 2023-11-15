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
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;

/**
 * Used to generically handle the core functionality of sending a GRPC request over Protostellar and handling the streaming
 * response.
 * <p>
 * Can handle any streaming setup, e.g. query, search, etc.
 * <p>
 * For single-request-single-response situations, {@see CoreProtostellarAccessors}.
 */
public class CoreProtostellarAccessorsStreaming {
  private CoreProtostellarAccessorsStreaming() {
  }

  public static <TGrpcRequest, TGrpcResponse>
  List<TGrpcResponse> blocking(CoreProtostellar core,
                               ProtostellarRequest<TGrpcRequest> request,
                               BiConsumer<ProtostellarEndpoint, StreamObserver<TGrpcResponse>> executeFutureGrpcCall,
                               Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    return async(core, request, executeFutureGrpcCall, convertException).toBlocking();
  }

  public static <TGrpcRequest, TGrpcResponse>
  CoreAsyncResponse<List<TGrpcResponse>> async(CoreProtostellar core,
                                               ProtostellarRequest<TGrpcRequest> request,
                                               BiConsumer<ProtostellarEndpoint, StreamObserver<TGrpcResponse>> executeFutureGrpcCall,
                                               Function<Throwable, ProtostellarRequestBehaviour> convertException) {

    CompletableFuture<List<TGrpcResponse>> ret = new CompletableFuture<>();
    CoreAsyncResponse<List<TGrpcResponse>> response = new CoreAsyncResponse<>(ret, () -> {
    });
    asyncInternal(ret, core, request, executeFutureGrpcCall, convertException);
    return response;
  }

  private static <TGrpcRequest, TGrpcResponse>
  void asyncInternal(CompletableFuture<List<TGrpcResponse>> ret,
                     CoreProtostellar core,
                     ProtostellarRequest<TGrpcRequest> request,
                     BiConsumer<ProtostellarEndpoint, StreamObserver<TGrpcResponse>> executeFutureGrpcCall,
                     Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    if (handleShutdownAsync(core, ret, request)) {
      return;
    }
    ProtostellarEndpoint endpoint = core.endpoint();
    List<TGrpcResponse> responses = new ArrayList<>();

    StreamObserver<TGrpcResponse> response = new StreamObserver<TGrpcResponse>() {
      @Override
      public void onNext(TGrpcResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        ProtostellarRequestBehaviour behaviour = convertException.apply(throwable);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            asyncInternal(ret, core, request, executeFutureGrpcCall, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            if (!request.completed()) {
              // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
              request.raisedResponseToUser(err);
              ret.completeExceptionally(err);
            }
          }
        } else {
          if (!request.completed()) {
            // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
            request.raisedResponseToUser(behaviour.exception());
            ret.completeExceptionally(behaviour.exception());
          }
        }
      }

      @Override
      public void onCompleted() {
        ret.complete(responses);
      }
    };

    executeFutureGrpcCall.accept(endpoint, response);
  }

  public static <TGrpcRequest, TGrpcResponse>
  Flux<TGrpcResponse> reactive(CoreProtostellar core,
                               ProtostellarRequest<TGrpcRequest> request,
                               BiConsumer<ProtostellarEndpoint, StreamObserver<TGrpcResponse>> executeFutureGrpcCall,
                               Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    return Flux.defer(() -> {
      Sinks.Many<TGrpcResponse> ret = Sinks.many().unicast().onBackpressureBuffer();
      reactiveInternal(ret, core, request, executeFutureGrpcCall, convertException);
      return ret.asFlux();
    });

  }

  private static <TGrpcRequest, TGrpcResponse>
  void reactiveInternal(Sinks.Many<TGrpcResponse> ret,
                        CoreProtostellar core,
                        ProtostellarRequest<TGrpcRequest> request,
                        BiConsumer<ProtostellarEndpoint, StreamObserver<TGrpcResponse>> executeFutureGrpcCall,
                        Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    if (handleShutdownReactive(ret, core, request)) {
      return;
    }
    ProtostellarEndpoint endpoint = core.endpoint();

    StreamObserver<TGrpcResponse> response = new StreamObserver<TGrpcResponse>() {
      @Override
      public void onNext(TGrpcResponse response) {
        ret.tryEmitNext(response).orThrow();
      }

      @Override
      public void onError(Throwable throwable) {
        ProtostellarRequestBehaviour behaviour = convertException.apply(throwable);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            reactiveInternal(ret, core, request, executeFutureGrpcCall, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            if (!request.completed()) {
              // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
              request.raisedResponseToUser(err);
              ret.tryEmitError(err).orThrow();
            }
          }
        } else {
          if (!request.completed()) {
            // The completed() check is just a sanity check - it shouldn't be possible to be retrying an operation that has already completed.
            request.raisedResponseToUser(behaviour.exception());
            ret.tryEmitError(behaviour.exception()).orThrow();
          }
        }
      }

      @Override
      public void onCompleted() {
        ret.tryEmitComplete();
      }
    };

    executeFutureGrpcCall.accept(endpoint, response);
  }
}
