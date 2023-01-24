/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.classic.query.EnhancedPreparedStatementStrategy;
import com.couchbase.client.core.classic.query.LegacyPreparedStatementStrategy;
import com.couchbase.client.core.classic.query.PreparedStatementStrategy;
import com.couchbase.client.core.cnc.events.request.PreparedStatementRetriedEvent;
import com.couchbase.client.core.config.ClusterCapabilities;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.couchbase.client.core.retry.RetryOrchestrator.capDuration;
import static java.util.Objects.requireNonNull;

@Stability.Internal
@Deprecated // Being replaced with CoreQueryOps
public class CoreQueryAccessor {
  /**
   * The maximum number of entries in the prepared statement cache.
   */
  private static final int PREPARED_STATEMENT_CACHE_SIZE = 5000;

  private volatile PreparedStatementStrategy strategy;

  public CoreQueryAccessor(Core core) {
    this.strategy = new LegacyPreparedStatementStrategy(core, PREPARED_STATEMENT_CACHE_SIZE);

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

  public Mono<QueryResponse> query(QueryRequest request, boolean adhoc) {
    if (adhoc) {
      return strategy.executeAdhoc(request);
    }

    return strategy.execute(request)
        .onErrorResume(PreparedStatementFailureException.class, new PreparedRetryFunction(request));
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

      final RetryReason retryReason = RetryReason.QUERY_PREPARED_STATEMENT_FAILURE;
      final CoreEnvironment env = request.context().environment();

      return Mono
          .fromFuture(request.retryStrategy().shouldRetry(request, retryReason))
          .flatMap(retryAction -> {
            Optional<Duration> duration = retryAction.duration();
            if (!duration.isPresent()) {
              return Mono.error(retryAction.exceptionTranslator().apply(t));
            }
            final Duration cappedDuration = capDuration(duration.get(), request);
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
