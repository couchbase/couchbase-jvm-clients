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

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.msg.query.PreparedStatement;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.util.LRUCache;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public abstract class PreparedStatementStrategy {
  private final Core core;
  protected final Map<String, PreparedStatement> cache; // key is query statement

  public PreparedStatementStrategy(Core core, int cacheSize) {
    this.core = requireNonNull(core);
    this.cache = Collections.synchronizedMap(new LRUCache<>(cacheSize));
  }

  protected RequestTracer requestTracer() {
    return core.context().coreResources().requestTracer();
  }

  public abstract Mono<QueryResponse> execute(QueryRequest request);

  /**
   * Executes a query using an existing prepared statement.
   */
  protected Mono<QueryResponse> executeAlreadyPrepared(QueryRequest request, PreparedStatement prepared) {
    return executeAdhoc(request.toExecuteRequest(prepared.name(), prepared.encodedPlan(), requestTracer()));
  }

  public Mono<QueryResponse> executeAdhoc(QueryRequest request) {
    return Mono.defer(() -> {
      core.send(request);
      return Reactor
          .wrap(request, request.response(), true)
          .doOnNext(ignored -> request.context().logicallyComplete())
          .doOnError(err -> request.context().logicallyComplete(err));
    });
  }

  public void evict(QueryRequest request) {
    cache.remove(request.statement());
  }
}
