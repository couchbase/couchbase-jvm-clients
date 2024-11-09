/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.CoreErrorCodeAndMessageException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.AnalyticsErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryAction;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.columnar.client.java.codec.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.couchbase.client.core.retry.AuthErrorDecider.getTlsHandshakeFailure;
import static com.couchbase.client.core.retry.RetryReason.AUTHENTICATION_ERROR;
import static com.couchbase.client.core.retry.RetryReason.ENDPOINT_NOT_AVAILABLE;
import static com.couchbase.client.core.retry.RetryReason.GLOBAL_CONFIG_LOAD_IN_PROGRESS;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.core.util.BlockingStreamingHelper.forEachBlocking;
import static com.couchbase.client.core.util.BlockingStreamingHelper.propagateAsCancellation;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class QueryExecutor {
  private static final Logger log = LoggerFactory.getLogger(QueryExecutor.class);

  private static final double dispatchTimeoutFactor = 1.5; // of connectTimeout

  private static final BackoffCalculator backoff = new BackoffCalculator(
    Duration.ofMillis(100),
    Duration.ofMinutes(1)
  );

  private final Core core;
  private final Environment environment;
  private final ColumnarRetryStrategy columnarRetryStrategy;

  public QueryExecutor(
    Core core,
    Environment environment,
    ConnectionString connectionString
  ) {
    this.core = requireNonNull(core);
    this.environment = requireNonNull(environment);

    Duration dispatchTimeout = Duration.ofNanos((long) (core.environment().timeoutConfig().connectTimeout().toNanos() * dispatchTimeoutFactor));
    columnarRetryStrategy = new ColumnarRetryStrategy(dispatchTimeout, connectionString);
  }

  QueryResult queryBuffered(
    String statement,
    Consumer<QueryOptions> optionsCustomizer,
    @Nullable CoreBucketAndScope scope
  ) {
    return doWithRetry(
      optionsCustomizer,
      (options, remainingTimeout) -> blockAndRewriteStackTrace(analyticsQueryAsync(
        core,
        analyticsRequest(statement, options, remainingTimeout, scope),
        defaultIfNull(options.deserializer(), environment.deserializer())
      ))
    );
  }

  QueryMetadata queryStreaming(
    String statement,
    Consumer<QueryOptions> optionsCustomizer,
    @Nullable CoreBucketAndScope scope,
    Consumer<Row> rowConsumer
  ) {
    return doWithRetry(
      optionsCustomizer,
      (options, remainingTimeout) -> analyticsQueryBlockingStreaming(
        core,
        analyticsRequest(statement, options, remainingTimeout, scope),
        defaultIfNull(options.deserializer(), environment.deserializer()),
        rowConsumer
      )
    );
  }

  /**
   * A little adapter that lets buffered and streaming queries
   * both use the same retry code.
   */
  private interface QueryStrategy<R> {
    R apply(
      QueryOptions.Unmodifiable options,
      Duration remainingTimeout
    );
  }

  private <R> R doWithRetry(
    Consumer<QueryOptions> optionsCustomizer,
    QueryStrategy<R> strategy
  ) {
    QueryOptions builder = new QueryOptions();
    optionsCustomizer.accept(builder);
    QueryOptions.Unmodifiable builtOpts = builder.build();

    Duration remainingTimeout = resolveTimeout(builtOpts);
    Deadline retryDeadline = Deadline.of(remainingTimeout);

    CoreErrorCodeAndMessageException prevError = null;
    int attempt = 0;

    while (true) {
      try {
        return strategy.apply(builtOpts, remainingTimeout);

      } catch (RuntimeException t) {
        if (t instanceof CoreErrorCodeAndMessageException) {
          CoreErrorCodeAndMessageException currentError = (CoreErrorCodeAndMessageException) t;

          if (currentError.retriable()) {
            Duration delay = backoff.delayForAttempt(attempt++);
            remainingTimeout = retryDeadline.remaining().orElse(Duration.ZERO);

            if (remainingTimeout.compareTo(delay) <= 0) {
              throw notEnoughTimeToRetry(attempt, currentError);
            }

            log.debug("Query attempt {} failed; retrying after {}. {}", attempt, delay, context(currentError));
            sleep(delay);
            prevError = currentError;
            continue; // retry!
          }
        }

        throw translateException(t, prevError);
      }
    }
  }

  private static TimeoutException notEnoughTimeToRetry(int attempt, CoreErrorCodeAndMessageException t) {
    TimeoutException timeoutException = new TimeoutException(
      "Query attempt " + attempt + " failed, and there's not enough time left to try again. " +
        t.context().exportAsString(Context.ExportFormat.JSON)
    );
    timeoutException.addSuppressed(translateException(t));
    return timeoutException;
  }

  private static Object context(CouchbaseException e) {
    // defers building the string unless actually needed [logged].
    return new Object() {
      @Override
      public String toString() {
        ErrorContext ctx = e.context();
        return ctx == null ? "{}" : ctx.exportAsString(Context.ExportFormat.JSON);
      }
    };
  }

  private static void sleep(Duration d) {
    try {
      MILLISECONDS.sleep(d.toMillis());
    } catch (InterruptedException e) {
      throw propagateAsCancellation(e);
    }
  }

  private static RuntimeException translateException(RuntimeException e, @Nullable Exception suppressMe) {
    RuntimeException result = translateException(e);
    if (suppressMe != null) {
      result.addSuppressed(suppressMe);
    }
    return result;
  }

  private static RuntimeException translateException(RuntimeException e) {
    if (e instanceof CoreErrorCodeAndMessageException) {
      CoreErrorCodeAndMessageException t = (CoreErrorCodeAndMessageException) e;

      if (t.hasCode(20000)) {
        return new InvalidCredentialException(t.context());
      }

      if (t.hasCode(21002)) {
        if (t.context() instanceof AnalyticsErrorContext) {
          AnalyticsErrorContext ctx = (AnalyticsErrorContext) t.context();
          if (ctx.requestContext().request().idempotent()) {
            return newSafeTimeoutException(t.context());
          }
        }
        return newAmbiguousTimeoutException(t.context());
      }

      ErrorCodeAndMessage primary = t.errors().stream()
        .filter(it -> !it.retry())
        .findFirst()
        .orElse(t.errors().get(0));

      return new QueryException(primary, t.context());
    }

    if (e instanceof com.couchbase.client.core.error.TimeoutException) {
      return e instanceof UnambiguousTimeoutException
        ? newSafeTimeoutException(((CouchbaseException) e).context())
        : newAmbiguousTimeoutException(((CouchbaseException) e).context());
    }

    // Columnar prefers native platform exceptions
    if (e instanceof InvalidArgumentException) {
      return new IllegalArgumentException(e.getMessage(), hide((CouchbaseException) e));
    }

    if (e instanceof CouchbaseException) {
      // Nothing from core-io is public Columnar API, not even exceptions!
      return hide((CouchbaseException) e);
    }

    return e;
  }

  Duration resolveTimeout(QueryOptions.Unmodifiable opts) {
    Duration customTimeout = opts.timeout();
    return customTimeout != null ? customTimeout : environment.timeoutConfig().analyticsTimeout();
  }

  /**
   * Helper method to craft an analytics request.
   *
   * @param statement the statement to use.
   * @param opts the built analytics options.
   * @return the created analytics request.
   */
  AnalyticsRequest analyticsRequest(
    final String statement,
    final QueryOptions.Unmodifiable opts,
    final Duration timeout,
    @Nullable final CoreBucketAndScope scope
  ) {
    requireNonNull(statement);

    // The server timeout doesn't _need_ to be different, but making it longer
    // means it's more likely the user will consistently get timeout exceptions
    // with client timeout error contexts instead of a mix of client- and server-side contexts.
    Duration serverTimeout = timeout.plus(Duration.ofSeconds(5));

    ObjectNode query = Mapper.createObjectNode();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(serverTimeout));
    if (scope != null) {
      query.put("query_context", "default:`" + scope.bucketName() + "`.`" + scope.scopeName() + "`");
    }
    opts.injectParams(query);

    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.get("client_context_id").asText();
    final RequestSpan span = null;

    int numericPriority = opts.priority() == QueryPriority.HIGH ? -1 : 0;

    CoreContext ctx = core.context();
    AnalyticsRequest request = new AnalyticsRequest(timeout, ctx, columnarRetryStrategy, ctx.authenticator(),
      queryBytes, numericPriority, opts.readOnly(), clientContextId, statement, span, null, null, false, 1
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  private static CompletableFuture<QueryResult> analyticsQueryAsync(
    final Core core,
    final AnalyticsRequest request,
    final Deserializer deserializer
  ) {
    return analyticsQueryInternal(core, request)
      .flatMap(response -> response
        .rows()
        .map(it -> new Row(it.data(), deserializer))
        .collectList()
        .flatMap(rows -> response
          .trailer()
          .map(trailer -> new QueryResult(response.header(), rows, trailer)
          )
        )
      )
      .timeout(
        request.timeout(),
        potentialTimeoutException(request)
      )
      .toFuture();
  }

  private static Mono<AnalyticsResponse> analyticsQueryInternal(final Core core, final AnalyticsRequest request) {
    core.send(request);
    return Reactor
      .wrap(request, request.response(), true)
      .doOnNext(ignored -> request.context().logicallyComplete())
      .doOnError(err -> request.context().logicallyComplete(err));
  }

  private static final int DEFAULT_STREAM_BUFFER_ROWS = 16;

  private static QueryMetadata analyticsQueryBlockingStreaming(
    final Core core,
    final AnalyticsRequest request,
    final Deserializer deserializer,
    final Consumer<Row> callback
  ) {
    Deadline deadline = Deadline.of(request.timeout());

    AnalyticsResponse response = analyticsQueryInternal(core, request).blockOptional().get();

    Mono<?> wholeStreamDeadlineAsMono = Mono.never().timeout(
      deadline.remaining().orElse(Duration.ZERO),
      potentialTimeoutException(request)
    );

    Flux<Row> rows = response.rows()
      .map(r -> new Row(r.data(), deserializer))
      .takeUntilOther(wholeStreamDeadlineAsMono);

    forEachBlocking(rows, DEFAULT_STREAM_BUFFER_ROWS, callback);

    try {
      return new QueryMetadata(response.header(), response.trailer().blockOptional().get());
    } catch (Exception e) {
      // in case the thread was interrupted immediately before blocking for the trailer
      CbThrowables.findCause(e, InterruptedException.class)
        .ifPresent(it -> {
          throw propagateAsCancellation(it);
        });
      throw e;
    }
  }

  private class ColumnarRetryStrategy extends BestEffortRetryStrategy {
    private final Duration dispatchTimeout;
    private final Deadline bootstrapDeadline;
    private final String bootstrapTimeoutMessage;
    private final String dispatchTimeoutMessage;
    private final boolean maybeCouchbaseInternalNonProd;

    public ColumnarRetryStrategy(
      Duration dispatchTimeout,
      ConnectionString connectionString
    ) {
      this.dispatchTimeout = requireNonNull(dispatchTimeout);
      this.bootstrapDeadline = Deadline.of(dispatchTimeout);

      this.bootstrapTimeoutMessage =
        "Failed to connect to cluster and get topology within " + dispatchTimeout + " (" + dispatchTimeoutFactor + " x connectTimeout)." +
          " Check connection string." +
          " If connecting to a hosted service, check the admin console and make sure this machine's IP is in the list of allowed IPs.";

      this.dispatchTimeoutMessage =
        "Failed to dispatch request within " + dispatchTimeout + " (" + dispatchTimeoutFactor + " x connectTimeout)." +
          " Check network status? Check cluster status?";

      this.maybeCouchbaseInternalNonProd = connectionString.hosts().get(0).host().endsWith(".nonprod-project-avengers.com");
    }

    private String tlsHandshakeErrorMessage(Throwable tlsHandshakeError) {
      String message = "A TLS handshake problem prevented the client from connecting to the server." +
        " Potential causes include the server (or an on-path attacker)" +
        " presenting a certificate the client is not configured to trust." +
        " If connecting to a hosted service, make sure to use a relatively recent" +
        " SDK version that has up-to-date certificates." +
        " Error message from the TLS engine: " + tlsHandshakeError;

      if (maybeCouchbaseInternalNonProd) {
        message = "It looks like you might be trying to connect to a Couchbase internal non-production hosted service." +
          " If this is true, please make sure you have configured the SDK to trust the non-prod certificate authority, like this:" +
          "\n\n" +
          "Cluster cluster = Cluster.newInstance(\n" +
          "  connectionString,\n" +
          "  Credential.of(username, password),\n" +
          "  clusterOptions -> clusterOptions\n" +
          "    .security(it -> it.trustOnlyCertificates(Certificates.getNonProdCertificates()))\n" +
          ");\n\n" +
          "We now return to your regularly scheduled exception message.\n\n"
          + message;
      }

      return message;
    }

    @Override
    public CompletableFuture<RetryAction> shouldRetry(
      Request<? extends Response> request,
      RetryReason reason
    ) {
      if (reason == AUTHENTICATION_ERROR) {
        // core-io uses this same retry reason for bad credentials and failed TLS handshake.
        // Disambiguate here!
        Throwable tlsHandshakeError = getTlsHandshakeFailure(core);
        if (tlsHandshakeError != null) {
          return failFast(it -> new RuntimeException(
            tlsHandshakeErrorMessage(tlsHandshakeError) + " " + getErrorContext(it, request),
            tlsHandshakeError
          ));
        }

        return failFast(it -> new InvalidCredentialException(getErrorContext(it, request)));
      }

      if (reason == GLOBAL_CONFIG_LOAD_IN_PROGRESS && bootstrapDeadline.exceeded()) {
        return failFast(it -> newSafeTimeoutException(bootstrapTimeoutMessage, getErrorContext(it, request)));
      }

      if (reason == ENDPOINT_NOT_AVAILABLE && dispatchTimeoutExpired(request)) {
        return failFast(it -> newSafeTimeoutException(dispatchTimeoutMessage, getErrorContext(it, request)));
      }

      return super.shouldRetry(request, reason);
    }

    private CompletableFuture<RetryAction> failFast(Function<Throwable, Throwable> exceptionTranslator) {
      return completedFuture(RetryAction.noRetry(exceptionTranslator));
    }

    private boolean dispatchTimeoutExpired(Request<?> request) {
      long nanosSinceCreation = System.nanoTime() - request.createdAt();
      return nanosSinceCreation > dispatchTimeout.toNanos();
    }

    private ErrorContext getErrorContext(Throwable t, Request<?> request) {
      if (t instanceof CouchbaseException) {
        CouchbaseException c = (CouchbaseException) t;
        if (c.context() != null) {
          return c.context();
        }
      }
      return new GenericRequestErrorContext(request);
    }
  }

  /**
   * Thrown when an operation times out with no side effects.
   * <p>
   * In other words, this exception tells you no data changed
   * on the server as a result of the timed-out operation.
   * <p>
   * Specifically, it is thrown in <em>either</em> of these cases:
   * <ul>
   *   <li>A read-only operation times out.
   *   <li>An write operation times out before the request
   *   is dispatched to the server.
   * </ul>
   *
   * @see QueryOptions#readOnly(Boolean)
   */
  private static TimeoutException newSafeTimeoutException(String message, Context context) {
    return new TimeoutException(message + " " + context.exportAsString(Context.ExportFormat.JSON));
  }

  private static TimeoutException newSafeTimeoutException(Context context) {
    return newSafeTimeoutException("The operation timed out. No data was changed on the server.", context);
  }

  private static TimeoutException newAmbiguousTimeoutException(Context context) {
    String message = "The operation timed out. It is unknown whether data was changed on the server.";
    return new TimeoutException(message + " " + context.exportAsString(Context.ExportFormat.JSON));
  }

  private static <T> Mono<T> potentialTimeoutException(AnalyticsRequest request) {
    // defer so we don't prematurely export request context to string.
    return Mono.defer(() -> Mono.error(() ->
      request.idempotent()
        ? newSafeTimeoutException(request.context())
        : newAmbiguousTimeoutException(request.context())));
  }

  /**
   * Replaces {@link CouchbaseException} with {@link RuntimeException},
   * to discourage users from depending on core-io while we plot to remove it.
   */
  private static RuntimeException hide(CouchbaseException e) {
    Throwable cause = e.getCause() instanceof CouchbaseException ? hide(((CouchbaseException) e.getCause())) : e.getCause();
    RuntimeException r = new RuntimeException(e.getClass().getSimpleName() + ": " + e.getMessage(), cause);
    r.setStackTrace(e.getStackTrace());

    for (Throwable t : e.getSuppressed()) {
      if (t instanceof CouchbaseException) {
        r.addSuppressed(hide((CouchbaseException) t));
      } else {
        r.addSuppressed(t);
      }
    }

    return r;
  }

  /**
   * Similar to {@link com.couchbase.client.core.util.CoreAsyncUtils#block(CompletableFuture)}
   * but handles interruption differently.
   */
  private static <T> T blockAndRewriteStackTrace(CompletableFuture<T> future) {
    try {
      return future.get();

    } catch (InterruptedException e) {
      throw propagateAsCancellation(e);

    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        rewriteStackTrace(cause);
        throw (RuntimeException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  /**
   * Adjusts the stack trace to point HERE instead of the thread where the exception was actually thrown.
   * Preserves the original async stack trace as a suppressed exception.
   */
  private static void rewriteStackTrace(Throwable t) {
    Exception suppressed = new Exception(
      "The above exception was originally thrown by another thread at the following location.");
    suppressed.setStackTrace(t.getStackTrace());
    t.fillInStackTrace();
    t.addSuppressed(suppressed);
  }
}
