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

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessorsStreaming;
import com.couchbase.client.core.protostellar.CoreProtostellarErrorHandlingUtil;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.query.v1.QueryRequest;
import com.couchbase.client.protostellar.query.v1.QueryResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedCurrentlyInProtostellar;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;


@Stability.Internal
public class ProtostellarCoreQueryOps implements CoreQueryOps {
  private final CoreProtostellar core;

  public ProtostellarCoreQueryOps(CoreProtostellar core) {
    this.core = requireNonNull(core);
  }

  @Override
  public CoreQueryResult queryBlocking(String statement,
                                       CoreQueryOptions options,
                                       @Nullable CoreQueryContext queryContext,
                                       @Nullable NodeIdentifier target,
                                       // errorConverter is unused, as it's only used for converting transactions
                                       // errors, which will be handled differently with Protostellar.
                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext, target);

    List<QueryResponse> responses = CoreProtostellarAccessorsStreaming.blocking(core,
            request,
            (endpoint, stream) -> {
              endpoint.queryStub()
                      .withDeadline(request.deadline())
                      .query(request.request(), stream);
              return null;
            },
            (error) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, error)
    );

    return new ProtostellarCoreQueryResult(responses);
  }

  @Override
  public CoreAsyncResponse<CoreQueryResult> queryAsync(String statement,
                                                       CoreQueryOptions options,
                                                       @Nullable CoreQueryContext queryContext,
                                                       @Nullable NodeIdentifier target,
                                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext, target);

    CoreAsyncResponse<List<QueryResponse>> responses = CoreProtostellarAccessorsStreaming.async(core,
      request,
      (endpoint, stream) -> {
        endpoint.queryStub()
          .withDeadline(request.deadline())
          .query(request.request(), stream);
        return null;
      },
      (error) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, error)
    );

    return responses.map(ProtostellarCoreQueryResult::new);
  }

  @Override
  public Mono<CoreReactiveQueryResult> queryReactive(String statement,
                                                     CoreQueryOptions options,
                                                     @Nullable CoreQueryContext queryContext,
                                                     @Nullable NodeIdentifier target,
                                                     @Nullable Function<Throwable, RuntimeException> errorConverter) {
    return Mono.defer(() -> {
      try {
        ProtostellarRequest<QueryRequest> request = request(core, statement, options, queryContext, target);

        Sinks.Many<QueryChunkRow> rows = Sinks.many().unicast().onBackpressureBuffer();
        Sinks.One<CoreQueryMetaData> metaData = Sinks.one();

        Flux<QueryResponse> responses = CoreProtostellarAccessorsStreaming.reactive(core,
          request,
          (endpoint, stream) -> {
            endpoint.queryStub()
              .withDeadline(request.deadline())
              .query(request.request(), stream);
            return null;
          },
          (error) -> CoreProtostellarErrorHandlingUtil.convertException(core, request, error)
        );

        responses.publishOn(core.context().environment().scheduler())
          .subscribe(response -> {
              response.getRowsList().forEach(row -> {
                rows.tryEmitNext(new QueryChunkRow(row.toByteArray())).orThrow();
              });

              if (response.hasMetaData()) {
                metaData.tryEmitValue(new ProtostellarCoreQueryMetaData(response.getMetaData())).orThrow();
              }
            },
            // Error has already been passed through CoreProtostellarErrorHandlingUtil in CoreProtostellarAccessorsStreaming
            throwable -> rows.tryEmitError(throwable).orThrow(),
            () -> rows.tryEmitComplete().orThrow());

        return Mono.just(new ProtostellarCoreReactiveQueryResult(rows.asFlux(), metaData.asMono()));
      } catch (Throwable err) {
        // Any errors from initial option validation.
        return Mono.error(err);
      }
    });
  }

  private static ProtostellarRequest<QueryRequest> request(CoreProtostellar core,
                                                           String statement,
                                                           CoreQueryOptions opts,
                                                           @Nullable CoreQueryContext queryContext,
                                                           @Nullable NodeIdentifier target) {
    if (target != null) {
      throw unsupportedInProtostellar("Targetting a specific query node");
    }

    if (opts.asTransaction()) {
      // Will be pushed down to GRPC QueryRequest eventually.
      throw unsupportedCurrentlyInProtostellar();
    }

    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));

    QueryRequest.Builder request = convertOptions(opts);

    request.setStatement(statement);
    if (queryContext != null) {
      request.setBucketName(queryContext.bucket());
      request.setScopeName(queryContext.scope());
    }

    Duration timeout = opts.commonOptions().timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_QUERY, CoreDurability.NONE, opts.commonOptions().parentSpan().orElse(null));
    span.attribute(TracingIdentifiers.ATTR_STATEMENT, statement);

    return new ProtostellarRequest<>(request.build(),
      core,
      ServiceType.QUERY,
      TracingIdentifiers.SPAN_REQUEST_QUERY,
      span,
      timeout,
      opts.readonly(),
      opts.commonOptions().retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.commonOptions().clientContext(),
      0,
      (ctx) -> {
        ctx.put("statement", RedactableArgument.redactMeta(statement));
      }
    );
  }

  static QueryRequest.ScanConsistency toProtostellar(CoreQueryScanConsistency scanConsistency) {
    return QueryRequest.ScanConsistency.valueOf("SCAN_CONSISTENCY_" + scanConsistency.name());
  }

  private static QueryRequest.Builder convertOptions(CoreQueryOptions opts) {
    QueryRequest.Builder input = QueryRequest.newBuilder();

    input.setClientContextId(opts.clientContextId() == null ? UUID.randomUUID().toString() : opts.clientContextId());

    if (opts.scanConsistency() != null) {
      input.setScanConsistency(toProtostellar(opts.scanConsistency()));
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
      input.setScanConsistency(QueryRequest.ScanConsistency.SCAN_CONSISTENCY_REQUEST_PLUS);
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
          input.setProfileMode(QueryRequest.ProfileMode.PROFILE_MODE_TIMINGS);
          break;
        case PHASES:
          input.setProfileMode(QueryRequest.ProfileMode.PROFILE_MODE_PHASES);
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
