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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.query.PreparedStatement;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import reactor.core.publisher.Mono;

/**
 * Server remembers query plan; client sends just the prepared statement name.
 * Client can prepare + execute with a single command.
 */
@Stability.Internal
public class EnhancedPreparedStatementStrategy extends PreparedStatementStrategy {

  public EnhancedPreparedStatementStrategy(Core core, int cacheSize) {
    super(core, cacheSize);
  }

  @Override
  public Mono<QueryResponse> execute(QueryRequest request) {
    return Mono.fromCallable(() -> cache.get(request.statement()))
        .flatMap(preparedStatement -> executeAlreadyPrepared(request, preparedStatement))
        .switchIfEmpty(prepareAndExecute(request));
  }

  private Mono<QueryResponse> prepareAndExecute(QueryRequest request) {
    return executeAdhoc(request.toPrepareRequest(true, requestTracer())) // auto-execute!
        .flatMap(queryResponse -> {
          // intercept the response and pluck out the prepared statement name
          String preparedName = queryResponse.header().prepared().orElse(null);
          if (preparedName == null) {
            return Mono.error(
                new CouchbaseException("Failed to locate prepared statement name in query response; this is a query bug!")
            );
          }
          cache.put(request.statement(), PreparedStatement.enhanced(preparedName));
          return Mono.just(queryResponse);
        });
  }
}
