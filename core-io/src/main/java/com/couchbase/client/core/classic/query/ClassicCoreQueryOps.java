/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryOptionsTransactions;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreQueryScanConsistency;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.request.PreparedStatementRetriedEvent;
import com.couchbase.client.core.config.ClusterCapabilities;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.util.Golang;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.retry.RetryOrchestrator.capDuration;
import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClassicCoreQueryOps implements CoreQueryOps {
  private final Core core;

  /**
   * The maximum number of entries in the prepared statement cache.
   */
  private static final int PREPARED_STATEMENT_CACHE_SIZE = 5000;

  private volatile PreparedStatementStrategy strategy;

  public ClassicCoreQueryOps(Core core) {
    this.core = core;
    this.strategy = new LegacyPreparedStatementStrategy(core, PREPARED_STATEMENT_CACHE_SIZE);

    // Asynchronously determine if we can use enhanced prepared statements with this cluster.
    core
        .configurationProvider()
        .configs()
        .filter(config -> {
          Set<ClusterCapabilities> caps = config.clusterCapabilities().get(ServiceType.QUERY);
          return caps != null && caps.contains(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS);
        })
        // the capability can't be rolled back once enabled, so stop listening after first event.
        .next()
        .subscribe(config ->
            // upgrade the strategy to take advantage of enhanced prepared statements
            this.strategy = new EnhancedPreparedStatementStrategy(core, PREPARED_STATEMENT_CACHE_SIZE));
  }

  private Mono<QueryResponse> query(QueryRequest request, boolean adhoc) {
    if (adhoc) {
      return strategy.executeAdhoc(request);
    }

    return strategy.execute(request)
        .onErrorResume(PreparedStatementFailureException.class, new PreparedRetryFunction(request));
  }

  @Override
  public CoreAsyncResponse<CoreQueryResult> queryAsync(String statement,
                                                       CoreQueryOptions options,
                                                       @Nullable CoreQueryContext queryContext,
                                                       @Nullable NodeIdentifier target,
                                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (options.asTransaction()) {
      CompletableFuture<CoreQueryResult> out = singleQueryTransactionBuffered(core, statement, options, queryContext, errorConverter).toFuture();
      return new CoreAsyncResponse(out, () -> {
      });
    } else {
      QueryRequest request = queryRequest(statement, options, queryContext, target);
      Mono<QueryResponse> result = query(request, options.adhoc());

      CompletableFuture<CoreQueryResult> out = result
          .onErrorMap(err -> {
            if (errorConverter != null) {
              return errorConverter.apply(err);
            }
            return err;
          })
          .flatMap(response -> response
              .rows()
              .collectList()
              .onErrorMap(err -> {
                if (errorConverter != null) {
                  return errorConverter.apply(err);
                }
                return err;
              })
              .flatMap(rows -> response
                  .trailer()
                  .map(trailer -> (CoreQueryResult) new ClassicCoreQueryResult(response.header(), rows, trailer, request.context().lastDispatchedToNode()))))
          .toFuture();

      return new CoreAsyncResponse<>(out, () -> {
      });
    }
  }

  @Override
  public Mono<CoreReactiveQueryResult> queryReactive(String statement,
                                                     CoreQueryOptions options,
                                                     @Nullable CoreQueryContext queryContext,
                                                     @Nullable NodeIdentifier target,
                                                     @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (options.asTransaction()) {
      return singleQueryTransactionReactive(statement, options, queryContext, errorConverter);
    } else {
      QueryRequest request = queryRequest(statement, options, queryContext, target);
      return query(request, options.adhoc())
          .map(v -> (CoreReactiveQueryResult) new ClassicCoreReactiveQueryResult(v, request.context().lastDispatchedToNode()))
          .onErrorMap(err -> {
            if (errorConverter != null) {
              return errorConverter.apply(err);
            }
            return err;
          });
    }
  }


  /**
   * Helper method to construct the query request.
   *
   * @param statement the statement of the query.
   * @param options the options.
   * @return the constructed query request.
   */
  private QueryRequest queryRequest(String statement,
                                    CoreQueryOptions options,
                                    @Nullable CoreQueryContext queryContext,
                                    @Nullable NodeIdentifier target) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));
    notNull(options, "options");

    Duration timeout = options.commonOptions().timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
    RetryStrategy retryStrategy = options.commonOptions().retryStrategy().orElse(core.context().environment().retryStrategy());

    ObjectNode query = convertOptions(options);
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    if (queryContext != null) {
      query.put("query_context", queryContext.format());
    }

    byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    RequestSpan span = core.context().coreResources()
        .requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_REQUEST_QUERY, options.commonOptions().parentSpan().orElse(null));

    QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), statement,
        queryBytes, options.readonly(), options.clientContextId(), span,
        queryContext == null ? null : queryContext.bucket(), queryContext == null ? null : queryContext.scope(), target);
    request.context().clientContext(options.commonOptions().clientContext());
    return request;
  }

  private static Mono<CoreQueryResult> singleQueryTransactionBuffered(Core core,
                                                                      String statement,
                                                                      CoreQueryOptions opts,
                                                                      @Nullable CoreQueryContext queryContext,
                                                                      @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (opts.commonOptions().retryStrategy().isPresent()) {
      // Transactions require control of the retry strategy
      throw new IllegalArgumentException("Cannot specify retryStrategy() if using asTransaction() on QueryOptions");
    }

    CoreTransactionsReactive tri = configureTransactions(core, opts);
    SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracer(), null,
            null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.commonOptions().parentSpan().map(SpanWrapper::new).orElse(null))
        .attribute(TracingIdentifiers.ATTR_STATEMENT, statement)
        .attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);

    // Don't want to pass down asTransaction, because we ultimately call back into CoreQueryOps and will end up in a recursive loop.
    CoreQueryOptionsTransactions shadowed = new CoreQueryOptionsTransactions(opts);
    shadowed.set(CoreQueryOptionsTransactions.QueryOptionsParameter.AS_TRANSACTION_OPTIONS, CoreQueryOptionsTransactions.ParameterPassthrough.ALWAYS_SHADOWED);

    return tri.queryBlocking(statement, queryContext, shadowed, Optional.of(span.span()))
        .onErrorResume(ex -> {
          // From a cluster.query() transaction the user will be expecting the traditional SDK errors.
          if (ex instanceof CoreTransactionExpiredException) {
            return Mono.error(new UnambiguousTimeoutException(ex.getMessage(), null));
          }
          if (errorConverter != null) {
            ex = errorConverter.apply(ex);
          }
          return Mono.error(ex);
        })
        .doOnError(err -> span.finish(err))
        .doOnTerminate(() -> span.finish());
  }

  private Mono<CoreReactiveQueryResult> singleQueryTransactionReactive(String statement,
                                                                       CoreQueryOptions opts,
                                                                       @Nullable CoreQueryContext queryContext,
                                                                       Function<Throwable, RuntimeException> errorConverter) {
    if (opts.commonOptions().retryStrategy().isPresent()) {
      // Transactions require control of the retry strategy
      throw new IllegalArgumentException("Cannot specify retryStrategy() if using asTransaction() on QueryOptions");
    }

    CoreTransactionsReactive tri = configureTransactions(core, opts);
    SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracer(), null,
            null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.commonOptions().parentSpan().map(SpanWrapper::new).orElse(null))
        .attribute(TracingIdentifiers.ATTR_STATEMENT, statement)
        .attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);

    // Don't want to pass down asTransaction, because we ultimately call back into CoreQueryOps and will end up in a recursive loop.
    CoreQueryOptionsTransactions shadowed = new CoreQueryOptionsTransactions(opts);
    shadowed.set(CoreQueryOptionsTransactions.QueryOptionsParameter.AS_TRANSACTION_OPTIONS, CoreQueryOptionsTransactions.ParameterPassthrough.ALWAYS_SHADOWED);

    return tri.query(statement, queryContext, shadowed, Optional.of(span.span()), errorConverter)
        .doOnError(err -> span.finish(err))
        .doOnTerminate(() -> span.finish());
  }

  private static CoreTransactionsReactive configureTransactions(Core core, CoreQueryOptions opts) {
    CoreSingleQueryTransactionOptions queryOpts = opts.asTransactionOptions();
    CoreTransactionsConfig transactionsConfig = core.context().environment().transactionsConfig();
    return new CoreTransactionsReactive(core,
        CoreTransactionsConfig.createForSingleQueryTransactions(queryOpts == null ? transactionsConfig.durabilityLevel() : queryOpts.durabilityLevel().orElse(transactionsConfig.durabilityLevel()),
            opts.commonOptions().timeout().orElse(transactionsConfig.transactionExpirationTime()),
            queryOpts == null ? null : queryOpts.attemptContextFactory().orElse(transactionsConfig.attemptContextFactory()),
            queryOpts == null ? transactionsConfig.metadataCollection() : queryOpts.metadataCollection(),
            core.environment().transactionsConfig().supported()));
  }

  @Stability.Internal
  public static ObjectNode convertOptions(CoreQueryOptions opts) {
    ObjectNode json = Mapper.createObjectNode();
    json.put("client_context_id", opts.clientContextId() == null ? UUID.randomUUID().toString() : opts.clientContextId());

    boolean positionalPresent = opts.positionalParameters() != null && !opts.positionalParameters().isEmpty();
    if (opts.namedParameters() != null && !opts.namedParameters().isEmpty()) {
      if (positionalPresent) {
        throw InvalidArgumentException.fromMessage("Both positional and named parameters cannot be present at the same time!");
      }

      opts.namedParameters().fields().forEachRemaining(param -> {
        String key = param.getKey();
        json.set(
            key.charAt(0) == '$' ? key : '$' + key,
            param.getValue()
        );
      });
    }

    if (positionalPresent) {
      json.put("args", opts.positionalParameters());
    }

    if (opts.scanConsistency() != null) {
      json.put("scan_consistency", opts.scanConsistency().toString());
    }

    if (opts.consistentWith() != null) {
      ObjectNode mutationState = Mapper.createObjectNode();
      for (MutationToken token : opts.consistentWith().tokens()) {
        ObjectNode bucket = (ObjectNode) mutationState.get(token.bucketName());
        if (bucket == null) {
          bucket = Mapper.createObjectNode();
          mutationState.put(token.bucketName(), bucket);
        }

        ArrayNode v = Mapper.createArrayNode();
        v.add(token.sequenceNumber());
        v.add(String.valueOf(token.partitionUUID()));
        bucket.put(String.valueOf(token.partitionID()), v);
      }
      json.put("scan_vectors", mutationState);
      json.put("scan_consistency", "at_plus");
    }

    if (opts.profile() != null) {
      json.put("profile", opts.profile().toString());
    }

    if (opts.scanWait() != null) {
      if (opts.scanConsistency() == null || CoreQueryScanConsistency.NOT_BOUNDED != opts.scanConsistency()) {
        json.put("scan_wait", Golang.encodeDurationToMs(opts.scanWait()));
      }
    }

    if (opts.maxParallelism() != null) {
      json.put("max_parallelism", opts.maxParallelism().toString());
    }

    if (opts.pipelineCap() != null) {
      json.put("pipeline_cap", opts.pipelineCap().toString());
    }

    if (opts.pipelineBatch() != null) {
      json.put("pipeline_batch", opts.pipelineBatch().toString());
    }

    if (opts.scanCap() != null) {
      json.put("scan_cap", opts.scanCap().toString());
    }

    if (!opts.metrics()) {
      json.put("metrics", false);
    }

    if (opts.readonly()) {
      json.put("readonly", true);
    }

    if (opts.flexIndex()) {
      json.put("use_fts", true);
    }

    if (opts.preserveExpiry() != null) {
      json.put("preserve_expiry", opts.preserveExpiry());
    }

    if (opts.useReplica() != null) {
      json.put("use_replica", opts.useReplica() ? "on" : "off");
    }

    JsonNode raw = opts.raw();
    if (raw != null) {
      for (Iterator<String> it = raw.fieldNames(); it.hasNext(); ) {
        String fieldName = it.next();
        json.set(fieldName, raw.get(fieldName));
      }
    }

    return json;
  }

  /**
   * This helper function encapsulates the retry handling logic when a prepared statement needs to be retried.
   * <p>
   * Note that this code uses, but also duplicates some functionality of the retry orchestrator, because
   * we need to handle retries but also need to execute different logic instead of "just" retrying a request.
   */
  private class PreparedRetryFunction implements Function<PreparedStatementFailureException, Mono<? extends QueryResponse>> {

    private final QueryRequest request;

    public PreparedRetryFunction(QueryRequest request) {
      this.request = requireNonNull(request);
    }

    @Override
    public Mono<? extends QueryResponse> apply(PreparedStatementFailureException t) {
      if (!t.retryable()) {
        return Mono.error(t);
      }

      strategy.evict(request);

      RetryReason retryReason = RetryReason.QUERY_PREPARED_STATEMENT_FAILURE;
      CoreEnvironment env = request.context().environment();

      return Mono
          .fromFuture(request.retryStrategy().shouldRetry(request, retryReason))
          .flatMap(retryAction -> {
            Optional<Duration> duration = retryAction.duration();
            if (!duration.isPresent()) {
              return Mono.error(retryAction.exceptionTranslator().apply(t));
            }
            Duration cappedDuration = capDuration(duration.get(), request);
            request.context().incrementRetryAttempts(cappedDuration, retryReason);
            env.eventBus().publish(
                new PreparedStatementRetriedEvent(cappedDuration, request.context(), retryReason, t)
            );
            return Mono
                .delay(cappedDuration, env.scheduler())
                .flatMap(l -> query(request, false));
          });
    }
  }
}
