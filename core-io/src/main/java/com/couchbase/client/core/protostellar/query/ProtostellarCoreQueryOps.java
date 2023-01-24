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
package com.couchbase.client.core.protostellar.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryProfile;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreQueryScanConsistency;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.query.v1.QueryRequest;
import com.couchbase.client.protostellar.query.v1.QueryResponse;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownBlocking;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;


@Stability.Internal
public class ProtostellarCoreQueryOps implements CoreQueryOps {
  private final Core core;

  public ProtostellarCoreQueryOps(Core core) {
    this.core = core;
  }

  @Override
  public CoreQueryResult queryBlocking(String statement,
                                       CoreQueryOptions options,
                                       @Nullable CoreQueryContext queryContext,
                                       @Nullable NodeIdentifier target,
                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (target != null) {
      throw unsupportedInProtostellar("Targetting a specific query node");
    }

    if (options.asTransaction()) {
      // Will be pushed down to QueryRequest eventually.
      throw unsupportedInProtostellar("Single query transactions");
    }

    ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext);
    handleShutdownBlocking(core, request);
    List<QueryResponse> responses = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<RuntimeException> err = new AtomicReference<>();

    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        err.set(convertException(errorConverter, throwable));
        latch.countDown();
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(request.deadline())
      .query(request.request(), response);

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (err.get() != null) {
      throw err.get();
    }

    return new ProtostellarCoreQueryResult(responses);
  }

  @Override
  public CoreAsyncResponse<CoreQueryResult> queryAsync(String statement,
                                                       CoreQueryOptions options,
                                                       @Nullable CoreQueryContext queryContext,
                                                       @Nullable NodeIdentifier target,
                                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (target != null) {
      throw unsupportedInProtostellar("Targetting a specific query node");
    }

    if (options.asTransaction()) {
      // Will be pushed down to QueryRequest eventually.
      throw unsupportedInProtostellar("Single query transactions");
    }

    ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext);
    CompletableFuture<CoreQueryResult> ret = new CompletableFuture<>();
    CoreAsyncResponse<CoreQueryResult> out = new CoreAsyncResponse<>(ret, () -> {
    });
    if (handleShutdownAsync(core, ret, request)) {
      return out;
    }
    List<QueryResponse> responses = new ArrayList<>();

    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        ret.completeExceptionally(convertException(errorConverter, throwable));
      }

      @Override
      public void onCompleted() {
        ret.complete(new ProtostellarCoreQueryResult(responses));
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(request.deadline())
      .query(request.request(), response);

    return out;
  }

  @Override
  public Mono<CoreReactiveQueryResult> queryReactive(String statement,
                                                     CoreQueryOptions options,
                                                     @Nullable CoreQueryContext queryContext,
                                                     @Nullable NodeIdentifier target,
                                                     @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (target != null) {
      throw unsupportedInProtostellar("Targetting a specific query node");
    }

    if (options.asTransaction()) {
      throw new IllegalStateException("Internal bug - calling code should have used singleQueryTransactionReactive instead");
    }

    ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext);
    Mono<CoreReactiveQueryResult> err = handleShutdownReactive(core, request);
    if (err != null) {
      return err;
    }

    Sinks.Many<QueryResponse> responses = Sinks.many().replay().latest();

    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.tryEmitNext(response).orThrow();
      }

      @Override
      public void onError(Throwable throwable) {
        responses.tryEmitError(convertException(errorConverter, throwable)).orThrow();
      }

      @Override
      public void onCompleted() {
        responses.tryEmitComplete().orThrow();
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(request.deadline())
      .query(request.request(), response);

    return Mono.just(new ProtostellarCoreReactiveQueryResult(responses.asFlux()));
  }

  private static RuntimeException convertException(@Nullable Function<Throwable, RuntimeException> errorConverter, Throwable throwable) {
    // STG does not currently implement most query errors.  Once it does, will want to pass throwable through CoreProtostellarErrorHandlingUtil.
    if (errorConverter != null) {
      return errorConverter.apply(throwable);
    }
    if (throwable instanceof RuntimeException) {
      return (RuntimeException) throwable;
    }
    return new RuntimeException(throwable);
  }

  private static ProtostellarRequest<QueryRequest> request(Core core,
                                                           String statement,
                                                           CoreQueryOptions opts,
                                                           @Nullable CoreQueryContext queryContext) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedAnalyticsErrorContext(statement));

    Duration timeout = opts.commonOptions().timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_QUERY, CoreDurability.NONE, opts.commonOptions().parentSpan().orElse(null));
    span.attribute(TracingIdentifiers.ATTR_STATEMENT, statement);
    ProtostellarRequest<QueryRequest> out = new ProtostellarRequest<>(core,
      ServiceType.QUERY,
      TracingIdentifiers.SPAN_REQUEST_QUERY,
      span,
      timeout,
      opts.readonly(),
      opts.commonOptions().retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext()
    );

    QueryRequest.Builder request = convertOptions(opts);

    request.setStatement(statement);
    if (queryContext != null) {
      request.setBucketName(queryContext.bucket());
      request.setScopeName(queryContext.scope());
    }

    out.request(request.build());

    return out;
  }

  private static QueryRequest.Builder convertOptions(CoreQueryOptions opts) {
    QueryRequest.Builder input = QueryRequest.newBuilder();

    input.setClientContextId(opts.clientContextId() == null ? UUID.randomUUID().toString() : opts.clientContextId());

    if (opts.scanConsistency() != null) {
      input.setScanConsistency(QueryRequest.QueryScanConsistency.valueOf(opts.scanConsistency().name()));
    }

    boolean positionalPresent = opts.positionalParameters() != null && !opts.positionalParameters().isEmpty();
    if (opts.namedParameters() != null && !opts.namedParameters().isEmpty()) {
      if (positionalPresent) {
        throw InvalidArgumentException.fromMessage("Both positional and named parameters cannot be present at the same time!");
      }

      opts.namedParameters().fieldNames().forEachRemaining(key -> {
        Object value = opts.namedParameters().get(key);
        try {
          ByteString bs = ByteString.copyFrom(Mapper.writer().writeValueAsBytes(value));
          input.putNamedParameters(key, bs);
        } catch (JsonProcessingException e) {
          throw new InvalidArgumentException("Unable to JSON encode named parameter " + key, e, null);
        }
      });
    }

    if (positionalPresent) {
      opts.positionalParameters().iterator().forEachRemaining(it -> {
        try {
          input.addPositionalParameters(ByteString.copyFrom(Mapper.writer().writeValueAsBytes(it)));
        } catch (JsonProcessingException e) {
          throw new InvalidArgumentException("Unable to JSON encode positional parameter " + it, e, null);
        }
      });
    }


    if (opts.scanConsistency() == CoreQueryScanConsistency.REQUEST_PLUS) {
      input.setScanConsistency(QueryRequest.QueryScanConsistency.REQUEST_PLUS);
    }

    if (opts.consistentWith() != null) {
      for (MutationToken token : opts.consistentWith().tokens()) {
        input.addConsistentWith(com.couchbase.client.protostellar.kv.v1.MutationToken.newBuilder()
          .setSeqNo(token.sequenceNumber())
          .setVbucketId(token.partitionID())
          .setVbucketUuid(token.partitionUUID())
          .setBucketName(token.bucketName())
          .build());
      }
    }

    if (opts.profile() != null && opts.profile() != CoreQueryProfile.OFF) {
      switch (opts.profile()) {
        case TIMINGS:
          input.setProfileMode(QueryRequest.QueryProfileMode.TIMINGS);
          break;
        case PHASES:
          input.setProfileMode(QueryRequest.QueryProfileMode.PHASES);
          break;
        default:
          throw new InvalidArgumentException("Unknown profile mode " + opts.profile(), null, null);
      }
    }

    QueryRequest.TuningOptions.Builder tuning = null;

    if (opts.scanWait() != null) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setScanWait(com.couchbase.client.core.deps.com.google.protobuf.Duration.newBuilder().setSeconds(TimeUnit.NANOSECONDS.toSeconds(opts.scanWait().toNanos())));
    }

    if (opts.maxParallelism() != null) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setMaxParallelism(opts.maxParallelism());
    }

    if (opts.pipelineCap() != null) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setPipelineCap(opts.pipelineCap());
    }

    if (opts.pipelineBatch() != null) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setPipelineBatch(opts.pipelineBatch());
    }

    if (opts.scanCap() != null) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setScanCap(opts.scanCap());
    }

    if (!opts.metrics()) {
      if (tuning == null) {
        tuning = QueryRequest.TuningOptions.newBuilder();
      }
      tuning.setDisableMetrics(!opts.metrics());
    }

    if (opts.readonly()) {
      input.setReadOnly(opts.readonly());
    }

    if (opts.flexIndex()) {
      input.setFlexIndex(opts.flexIndex());
    }

    if (opts.preserveExpiry() != null) {
      input.setPreserveExpiry(opts.preserveExpiry());
    }

    if (!opts.adhoc()) {
      input.setPrepared(true);
    }

    JsonNode raw = opts.raw();
    if (raw != null && !raw.isEmpty()) {
      throw new UnsupportedOperationException("Raw options cannot be used together with Protostellar");
    }

    if (tuning != null) {
      input.setTuningOptions(tuning);
    }

    return input;
  }
}
