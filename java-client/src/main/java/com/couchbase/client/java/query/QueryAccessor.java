/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.events.request.PreparedStatementRetriedEvent;
import com.couchbase.client.core.config.ClusterCapabilities;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.LRUCache;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.retry.RetryOrchestrator.capDuration;
import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;

/**
 * Converts requests and responses for N1QL queries.
 *
 * <p>Note that this accessor also transparently deals with prepared statements and the associated query
 * cache.</p>
 *
 * <p>Also, this class has internal functionality and is not intended to be called from the user directly.</p>
 */
@Stability.Internal
public class QueryAccessor {

    /**
     * The maximum number of prepared queries that will be kept around if the cache is enabled.
     */
    private static final int QUERY_CACHE_SIZE = 5000;

    /**
     * Holds the query cache.
     */
    private final Map<String, QueryCacheEntry> queryCache = Collections.synchronizedMap(
      new LRUCache<>(QUERY_CACHE_SIZE)
    );

    private final Core core;

    /**
     * Caches the value if enhanced prepared is enabled for fastpath config checking.
     */
    private volatile boolean enhancedPreparedEnabled = false;

    public QueryAccessor(final Core core) {
        this.core = core;

        core
          .configurationProvider()
          .configs()
          .subscribe(this::updateEnhancedPreparedEnabled);
    }

    /**
     * Helper method to calculate if prepared statements are enabled or not.
     *
     * <p>Note that once it is enabled it cannot roll back, so we can bail out quickly once we found
     * out that it is enabled.</p>
     *
     * @param config the config to check.
     */
    private void updateEnhancedPreparedEnabled(final ClusterConfig config) {
        if (enhancedPreparedEnabled) {
            return;
        }

        Set<ClusterCapabilities> caps = config.clusterCapabilities().get(ServiceType.QUERY);
        enhancedPreparedEnabled = caps != null && caps.contains(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS);
    }

    /**
     * Performs a N1QL query and returns the result as a future.
     *
     * <p>Note that compared to the reactive method, this one collects the rows into a list and makes sure
     * everything is part of the result. If you need backpressure, go with reactive.</p>
     *
     * @param request the request to perform.
     * @param options query options to use.
     * @return the future once the result is complete.
     */
    public CompletableFuture<QueryResult> queryAsync(final QueryRequest request, final QueryOptions.Built options,
                                                     final JsonSerializer serializer) {
        return queryInternal(request, options, options.adhoc(), serializer)
          .flatMap(response -> response
            .rows()
            .collectList()
            .flatMap(rows -> response
                .trailer()
                .map(trailer -> new QueryResult(response.header(), rows, trailer, serializer))
            )
          )
          .toFuture();
    }

    /**
     * Performs a N1QL query and returns the result as a future.
     *
     * @param request the request to perform.
     * @param options query options to use.
     * @return the mono once the result is complete.
     */
    public Mono<ReactiveQueryResult> queryReactive(final QueryRequest request, final QueryOptions.Built options,
                                                   final JsonSerializer serializer) {
        return queryInternal(request, options, options.adhoc(), serializer).map(r -> new ReactiveQueryResult(r, serializer));
    }

    /**
     * Internal method to dispatch the request into the core and return it as a mono.
     *
     * @param request the request to perform.
     * @param options query options to use.
     * @param adhoc if this query is adhoc.
     * @return the mono once the result is complete.
     */
    private Mono<QueryResponse> queryInternal(final QueryRequest request, final QueryOptions.Built options,
                                              final boolean adhoc, final JsonSerializer serializer) {
        if (adhoc) {
            core.send(request);
            return Reactor
              .wrap(request, request.response(), true)
              .doFinally(signalType -> request.context().logicallyComplete());
        } else {
            return maybePrepareAndExecute(request, options, serializer)
              .doFinally(signalType -> request.context().logicallyComplete());
        }
    }

    /**
     * Main method to drive the prepare and execute cycle.
     *
     * <p>Depending on if the statement is already cached, this method checks if a prepare needs to be executed,
     * and if so does it. In both cases, afterwards a subsequent execute is conducted with the primed cache and
     * the options that were present in the original query.</p>
     *
     * <p>The code also checks if the cache entry is still valid, to handle the upgrade scenario an potentially
     * flush the cache entry in this case to then execute with the newer approach.</p>
     *
     * @param request the request to perform.
     * @param options query options to use.
     * @return the mono once the result is complete.
     */
    private Mono<QueryResponse> maybePrepareAndExecute(final QueryRequest request, final QueryOptions.Built options,
                                                       final JsonSerializer serializer) {
        final QueryCacheEntry cacheEntry = queryCache.get(request.statement());
        boolean enhancedEnabled = enhancedPreparedEnabled;

        if (cacheEntry != null && cacheEntryStillValid(cacheEntry, enhancedEnabled)) {
            return queryInternal(buildExecuteRequest(cacheEntry, request, options), options, true, serializer)
                .onErrorResume(new PreparedRetryFunction(request, options, serializer));
        } else if (enhancedEnabled) {
            return queryInternal(buildPrepareRequest(request, options), options, true, serializer)
              .flatMap(qr -> {
                  Optional<String> preparedName = qr.header().prepared();
                  if (!preparedName.isPresent()) {
                      return Mono.error(
                        new CouchbaseException("No prepared name present but must be, this is a query bug!")
                      );
                  }
                  queryCache.put(
                    request.statement(),
                    new QueryCacheEntry(false, null, preparedName.get())
                  );
                  return Mono.just(qr);
              });
        } else {
            return queryReactive(buildPrepareRequest(request, options), queryOptions().build(), serializer)
              .flatMap(result -> result.rowsAsObject().next())
              .map(row -> {
                  queryCache.put(
                    request.statement(),
                    new QueryCacheEntry(
                      true,
                      row.getString("encoded_plan"),
                      row.getString("name")
                    )
                  );
                  return row;
              })
              .then(Mono.defer(() -> maybePrepareAndExecute(request, options, serializer)));
        }
    }

    /**
     * Builds the request to prepare a prepared statement.
     *
     * @param original the original request from which params are extracted.
     * @return the created request, ready to be sent over the wire.
     */
    private QueryRequest buildPrepareRequest(final QueryRequest original, final QueryOptions.Built options) {
        String statement = "PREPARE " + original.statement();

        JsonObject query = JsonObject.create();
        query.put("statement", statement);
        query.put("timeout", encodeDurationToMs(original.timeout()));
        query.put(
          "client_context_id",
          options.clientContextId() != null ? options.clientContextId() : UUID.randomUUID().toString()
        );

        if (enhancedPreparedEnabled) {
            query.put("auto_execute", true);
            options.injectParams(query);
        }


        InternalSpan span = core.context().environment().requestTracer()
          .internalSpan("prepare", original.internalSpan().toRequestSpan());

        return new QueryRequest(
          original.timeout(),
          original.context(),
          original.retryStrategy(),
          original.credentials(),
          statement,
          query.toString().getBytes(StandardCharsets.UTF_8),
          true,
          query.getString("client_context_id"),
          span
        );
    }

    /**
     * Constructs the execute request from the primed cache and the original request options.
     *
     * @param cacheEntry the primed cache entry.
     * @param original the original request.
     * @param originalOptions the original request options.
     * @return the created request, ready to be sent over the wire.
     */
    private QueryRequest buildExecuteRequest(final QueryCacheEntry cacheEntry, final QueryRequest original,
                                             final QueryOptions.Built originalOptions) {
        JsonObject query = cacheEntry.export();
        query.put("timeout", encodeDurationToMs(original.timeout()));
        originalOptions.injectParams(query);

        InternalSpan span = core.context().environment().requestTracer()
          .internalSpan("execute", original.internalSpan().toRequestSpan());

        return new QueryRequest(
          original.timeout(),
          original.context(),
          original.retryStrategy(),
          original.credentials(),
          original.statement(),
          query.toString().getBytes(StandardCharsets.UTF_8),
          originalOptions.readonly(),
          query.getString("client_context_id"),
          span
        );
    }

    /**
     * If an upgrade has happened and we can now do enhanced prepared, the cache got invalid.
     *
     * @param entry the entry to check.
     * @param enhancedEnabled if enhanced prepared statementd are enabled.
     * @return true if still valid, false otherwise.
     */
    private boolean cacheEntryStillValid(final QueryCacheEntry entry, final boolean enhancedEnabled) {
        return (enhancedEnabled && !entry.fullPlan) || (!enhancedEnabled && entry.fullPlan);
    }

    /**
     * Holds a cache entry, which might either be the full plan or just the name, depending on the
     * cluster state.
     */
    private static class QueryCacheEntry {

        private final String name;
        private final boolean fullPlan;
        private final String value;

        QueryCacheEntry(final boolean fullPlan, final String value, final String name) {
            this.fullPlan = fullPlan;
            this.value = value;
            this.name = name;
        }

        JsonObject export() {
            JsonObject result = JsonObject.create();
            result.put("prepared", name);
            if (fullPlan) {
                result.put("encoded_plan", value);
            }
            return result;
        }
    }

    /**
     * This helper function encapsulates the retry handling logic when a prepared statement needs to be retried.
     * <p>
     * Note that this code uses, but also duplicates some of the functionality of the retry orchestrator, because
     * we need to handle retries but also need to execute different logic instead of "just" retrying a request.
     */
    private class PreparedRetryFunction implements Function<Throwable, Mono<? extends QueryResponse>> {

        private final QueryRequest request;
        private final QueryOptions.Built options;
        private final JsonSerializer serializer;

        public PreparedRetryFunction(final QueryRequest request, final QueryOptions.Built options,
                                     final JsonSerializer serializer) {
            this.request = request;
            this.options = options;
            this.serializer = serializer;
        }

        @Override
        public Mono<? extends QueryResponse> apply(final Throwable t) {
            if (t instanceof PreparedStatementFailureException) {
                if (((PreparedStatementFailureException) t).retryable()) {
                    queryCache.remove(request.statement());

                    final RetryReason retryReason = RetryReason.QUERY_PREPARED_STATEMENT_FAILURE;
                    final CoreEnvironment env = request.context().environment();

                    return Mono
                      .fromFuture(request.retryStrategy().shouldRetry(request, retryReason))
                      .flatMap(retryAction -> {
                          Optional<Duration> duration = retryAction.duration();
                          if (duration.isPresent()) {
                              final Duration cappedDuration = capDuration(duration.get(), request);
                              request.context().incrementRetryAttempts(cappedDuration, retryReason);
                              env.eventBus().publish(
                                new PreparedStatementRetriedEvent(cappedDuration, request.context(), retryReason, t)
                              );
                              return Mono
                                .delay(cappedDuration, env.scheduler())
                                .flatMap(l -> maybePrepareAndExecute(request, options, serializer));
                          } else {
                              return Mono.error(t);
                          }
                      });
                }
            }
            return Mono.error(t);
        }
    }

}
