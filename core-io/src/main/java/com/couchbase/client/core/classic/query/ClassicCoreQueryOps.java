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
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryOptionsTransactions;
import com.couchbase.client.core.api.query.CoreQueryProfile;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreQueryScanConsistency;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.request.PreparedStatementRetriedEvent;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
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
import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.query.QueryChunkRow;
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
import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.client.core.util.Golang;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.couchbase.client.core.retry.RetryOrchestrator.capDuration;
import static com.couchbase.client.core.util.BlockingStreamingHelper.forEachBlocking;
import static com.couchbase.client.core.util.CbThrowables.reinterruptAndWrap;
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

  /**
   * Whether blocking queries do their work on the calling thread (see {@link #queryBlocking}).
   * True unless the {@code com.couchbase.blockingQueryOnCallerThread} system property is false.
   */
  private static final boolean BLOCKING_QUERY_ON_CALLER_THREAD =
      Boolean.parseBoolean(System.getProperty("com.couchbase.blockingQueryOnCallerThread", "true"));

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

    // The original request is never dispatched; the strategy sends copies of it.
    // End the original's span here, after any prepared statement retries.
    return executePrepared(request)
        .doOnNext(ignored -> request.context().endSpan(null))
        .doOnError(err -> request.context().endSpan(err));
  }

  private Mono<QueryResponse> executePrepared(QueryRequest request) {
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
      CompletableFuture<CoreQueryResult> out = bufferedResult(request, options.adhoc(), errorConverter);

      return new CoreAsyncResponse<>(out, () -> {
      });
    }
  }

  /**
   * Like the default implementation, but the calling thread does the work itself (see {@link BlockingCaller}):
   * sends the request, reads the response, and waits out retry delays.
   * <p>
   * For a prepared statement (a query that isn't adhoc), the request that's sent is a copy of the original request,
   * to prepare the statement (and execute it), or to execute it if it's already prepared; the copy has the same
   * blocking caller. Only with enhanced prepared statements, though, which send one request per query.
   * Otherwise (with servers that don't support them), a query that isn't prepared yet sends a request to prepare it,
   * then, while reading that response, a request to execute it, which on the same thread would mean sending the second
   * request before the first one's done. So prepared statements without enhanced prepared statements use the default
   * implementation, as do single-query transactions.
   */
  @Override
  public CoreQueryResult queryBlocking(String statement,
                                       CoreQueryOptions options,
                                       @Nullable CoreQueryContext queryContext,
                                       @Nullable NodeIdentifier target,
                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    if (!worksOnCallerThread(options)) {
      return CoreQueryOps.super.queryBlocking(statement, options, queryContext, target, errorConverter);
    }

    List<QueryChunkRow> rows = new ArrayList<>();
    try {
      CoreQueryMetaData metaData = queryBlockingStreaming(statement, options, queryContext, target, errorConverter, rows::add);
      return new ClassicCoreQueryResult(rows, (ClassicCoreQueryMetaData) metaData);

    } catch (RuntimeException e) {
      if (e.getCause() instanceof InterruptedException) {
        // Unlike queryBlockingStreaming, this has always left an interrupted thread's interrupted flag cleared
        // (like an interrupted Future.get()). Todo: leave it set (JVMCBC-1555).
        Thread.interrupted();
      }
      throw e;
    }
  }

  /**
   * Like the default implementation, but the calling thread does the work itself, as in {@link #queryBlocking},
   * and passes each row to the callback, on the calling thread.
   * <p>
   * The service passes each row to the callback as it reads the response (see {@link BlockingCaller#rowAction}).
   * If another thread reads the response anyway (for example, a prepared query retried on another thread),
   * the rows arrive through the response's stream of rows, and this passes them to the callback.
   * <p>
   * If the thread is interrupted, the query stops (at the next row, if it's reading the rows), and this throws
   * {@code RuntimeException(InterruptedException)}, with what the interruption caused (if anything) suppressed
   * on the {@code InterruptedException}, and leaves the thread interrupted. Unless the result is already complete:
   * then this returns its metadata, and leaves the thread interrupted, like the blocking caller.
   */
  @Override
  public CoreQueryMetaData queryBlockingStreaming(String statement,
                                                  CoreQueryOptions options,
                                                  @Nullable CoreQueryContext queryContext,
                                                  @Nullable NodeIdentifier target,
                                                  @Nullable Function<Throwable, RuntimeException> errorConverter,
                                                  Consumer<QueryChunkRow> callback) {
    if (!worksOnCallerThread(options)) {
      return CoreQueryOps.super.queryBlockingStreaming(statement, options, queryContext, target, errorConverter, callback);
    }

    BlockingCaller caller = new BlockingCaller();
    caller.rowAction((QueryChunkRow row) -> {
      // Like reading the rows from a stream, which stops (and says why) once it notices the thread was interrupted.
      if (Thread.currentThread().isInterrupted()) {
        throw reinterruptAndWrap(new InterruptedException());
      }
      callback.accept(row);
    });
    QueryRequest request = queryRequest(statement, options, queryContext, target);
    request.blockingCaller(caller);

    try {
      CoreReactiveQueryResult result = caller.sendAndAwait(() ->
          reactiveResult(request, options.adhoc(), errorConverter).toFuture());

      if (caller.rowsWentToAction()) {
        Throwable rowActionFailure = caller.rowActionFailure();
        if (rowActionFailure != null) {
          throw CbThrowables.propagate(rowActionFailure);
        }
        // The rows already ended. Checks how, without waiting: an interrupt now doesn't stop a result that's complete.
        Throwable rowsFailure = endOfRows(result.rows());
        if (rowsFailure != null) {
          throw CbThrowables.propagate(convertError(rowsFailure, errorConverter));
        }
      } else {
        forEachBlocking(result.rows().onErrorMap(err -> convertError(err, errorConverter)), 16, callback, CbThrowables::reinterruptAndWrap);
      }

      // Arrives after the rows. Already here if this thread read the response.
      return requireNonNull(result.metaData().block());

    } catch (RuntimeException e) {
      // The caller was interrupted while it waited (or while it read the response, which made reading fail).
      CbThrowables.findCause(e, InterruptedException.class)
          .ifPresent(it -> { throw reinterruptAndWrap(it); });
      if (Thread.currentThread().isInterrupted()) {
        // Reading failed because of the interrupt. Like the blocking caller, report what it caused as a suppressed exception.
        InterruptedException cause = new InterruptedException();
        cause.addSuppressed(e);
        throw reinterruptAndWrap(cause);
      }
      throw e;
    }
  }

  /**
   * Returns how a stream of rows that already ended ended: null if successfully, otherwise the failure.
   */
  private static @Nullable Throwable endOfRows(Flux<QueryChunkRow> rows) {
    AtomicBoolean ended = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    rows.subscribe(
        row -> {
          // None: they went to the row action.
        },
        error -> {
          failure.set(error);
          ended.set(true);
        },
        () -> ended.set(true)
    );
    if (!ended.get()) {
      throw new IllegalStateException("Expected the rows to have ended already");
    }
    return failure.get();
  }

  private static Throwable convertError(Throwable err, @Nullable Function<Throwable, RuntimeException> errorConverter) {
    return errorConverter == null ? err : errorConverter.apply(err);
  }

  /**
   * Whether a blocking query can do its work on the calling thread (see {@link #queryBlocking}).
   */
  private boolean worksOnCallerThread(CoreQueryOptions options) {
    return BLOCKING_QUERY_ON_CALLER_THREAD
        && !options.asTransaction()
        // The strategy only ever changes from legacy to enhanced, so the query uses enhanced if this sees it.
        && (options.adhoc() || strategy instanceof EnhancedPreparedStatementStrategy);
  }

  /**
   * Sends the request, and returns the result with all its rows.
   */
  private CompletableFuture<CoreQueryResult> bufferedResult(QueryRequest request,
                                                            boolean adhoc,
                                                            @Nullable Function<Throwable, RuntimeException> errorConverter) {
    return query(request, adhoc)
        .onErrorMap(err -> convertError(err, errorConverter))
        .flatMap(response -> response
            .rows()
            .collectList()
            .onErrorMap(err -> convertError(err, errorConverter))
            .flatMap(rows -> response
                .trailer()
                .map(trailer -> (CoreQueryResult) new ClassicCoreQueryResult(response.header(), rows, trailer, request.context().lastDispatchedToNode()))))
        .toFuture();
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
      return reactiveResult(request, options.adhoc(), errorConverter);
    }
  }

  /**
   * Sends the request, and returns the result, whose rows arrive later.
   */
  private Mono<CoreReactiveQueryResult> reactiveResult(QueryRequest request,
                                                       boolean adhoc,
                                                       @Nullable Function<Throwable, RuntimeException> errorConverter) {
    return query(request, adhoc)
        .map(v -> (CoreReactiveQueryResult) new ClassicCoreReactiveQueryResult(v, request.context().lastDispatchedToNode()))
        .onErrorMap(err -> convertError(err, errorConverter));
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

    ArrayNode positionalParameters = options.positionalParameters();
    ObjectNode namedParameters = options.namedParameters();
    boolean parametersUsed = (positionalParameters != null && !positionalParameters.isEmpty())
        || (namedParameters != null && !namedParameters.isEmpty());
    QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), statement,
        queryBytes, options.readonly(), options.clientContextId(), span,
        queryContext == null ? null : queryContext.bucket(), queryContext == null ? null : queryContext.scope(), target, parametersUsed);
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
    TracingDecorator tip = core.coreResources().tracingDecorator();
    SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), null,
            null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.commonOptions().parentSpan().map(SpanWrapper::new).orElse(null));
    tip.provideAttr(TracingAttribute.TRANSACTION_SINGLE_QUERY, span.span(), true);

    boolean parametersUsed = opts.positionalParameters() != null || opts.namedParameters() != null;
    tip.provideQueryStatementIfSafe(TracingAttribute.STATEMENT, span.span(), statement, parametersUsed);

    // Don't want to pass down asTransaction, because we ultimately call back into CoreQueryOps and will end up in a recursive loop.
    CoreQueryOptionsTransactions shadowed = new CoreQueryOptionsTransactions(opts);
    shadowed.set(CoreQueryOptionsTransactions.QueryOptionsParameter.AS_TRANSACTION_OPTIONS, CoreQueryOptionsTransactions.ParameterPassthrough.ALWAYS_SHADOWED);

    return tri.queryBlocking(statement, queryContext, shadowed, Optional.of(span.span()))
        .onErrorResume(ex -> {
          // From a cluster.query() transaction the user will be expecting the traditional SDK errors.
          if (ex instanceof CoreTransactionExpiredException) {
            return Mono.error(new UnambiguousTimeoutException(ex.getMessage(), null));
          }
          return Mono.error(convertError(ex, errorConverter));
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
    SpanWrapper span = SpanWrapperUtil.createOp(null, core.context().coreResources().requestTracerAndDecorator(), null,
            null, TracingIdentifiers.SPAN_REQUEST_QUERY, opts.commonOptions().parentSpan().map(SpanWrapper::new).orElse(null));

    core.coreResources().tracingDecorator().provideAttr(TracingAttribute.TRANSACTION_SINGLE_QUERY, span.span(), true);

    boolean parametersUsed = opts.positionalParameters() != null || opts.namedParameters() != null;
    core.coreResources().tracingDecorator().provideQueryStatementIfSafe(TracingAttribute.STATEMENT, span.span(), statement, parametersUsed);

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
    String clientContextId = opts.clientContextId();
    json.put("client_context_id", clientContextId == null ? UUID.randomUUID().toString() : clientContextId);

    ArrayNode positionalParameters = opts.positionalParameters();
    ObjectNode namedParameters = opts.namedParameters();
    boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
    if (namedParameters != null && !namedParameters.isEmpty()) {
      if (positionalPresent) {
        throw InvalidArgumentException.fromMessage("Both positional and named parameters cannot be present at the same time!");
      }

      namedParameters.fields().forEachRemaining(param -> {
        String key = param.getKey();
        json.set(
            key.charAt(0) == '$' ? key : '$' + key,
            param.getValue()
        );
      });
    }

    if (positionalPresent) {
      json.put("args", positionalParameters);
    }

    CoreQueryScanConsistency scanConsistency = opts.scanConsistency();
    if (scanConsistency != null) {
      json.put("scan_consistency", scanConsistency.toString());
    }

    CoreMutationState consistentWith = opts.consistentWith();
    if (consistentWith != null) {
      ObjectNode mutationState = Mapper.createObjectNode();
      for (MutationToken token : consistentWith.tokens()) {
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

    CoreQueryProfile profile = opts.profile();
    if (profile != null) {
      json.put("profile", profile.toString());
    }

    Duration scanWait = opts.scanWait();
    if (scanWait != null) {
      if (CoreQueryScanConsistency.NOT_BOUNDED != scanConsistency) {
        json.put("scan_wait", Golang.encodeDurationToMs(scanWait));
      }
    }

    Integer maxParallelism = opts.maxParallelism();
    if (maxParallelism != null) {
      json.put("max_parallelism", maxParallelism.toString());
    }

    Integer pipelineCap = opts.pipelineCap();
    if (pipelineCap != null) {
      json.put("pipeline_cap", pipelineCap.toString());
    }

    Integer pipelineBatch = opts.pipelineBatch();
    if (pipelineBatch != null) {
      json.put("pipeline_batch", pipelineBatch.toString());
    }

    Integer scanCap = opts.scanCap();
    if (scanCap != null) {
      json.put("scan_cap", scanCap.toString());
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

    Boolean preserveExpiry = opts.preserveExpiry();
    if (preserveExpiry != null) {
      json.put("preserve_expiry", preserveExpiry);
    }

    Boolean useReplica = opts.useReplica();
    if (useReplica != null) {
      json.put("use_replica", useReplica ? "on" : "off");
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
                .flatMap(l -> executePrepared(request));
          });
    }
  }
}
