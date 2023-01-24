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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.query.PreparedStatement;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import reactor.core.publisher.Mono;

/**
 * Client remembers query plan and sends it on every request.
 * Prepare and execute are separate commands.
 */
@Stability.Internal
public class LegacyPreparedStatementStrategy extends PreparedStatementStrategy {

  public LegacyPreparedStatementStrategy(Core core, int cacheSize) {
    super(core, cacheSize);
  }

  @Override
  public Mono<QueryResponse> execute(QueryRequest request) {
    return Mono.fromCallable(() -> cache.get(request.statement()))
        .switchIfEmpty(prepare(request))
        .flatMap(preparedStatement -> executeAlreadyPrepared(request, preparedStatement));
  }

  private Mono<PreparedStatement> prepare(QueryRequest originalRequest) {
    return executeAdhoc(originalRequest.toPrepareRequest(false, requestTracer()))
        .flatMap(queryResponse -> queryResponse.rows().next())
        .flatMap(row -> {
          ObjectNode node = (ObjectNode) Mapper.decodeIntoTree(row.data());
          PreparedStatement prepared = PreparedStatement.legacy(
              node.get("name").textValue(),
              node.get("encoded_plan").textValue());
          cache.put(originalRequest.statement(), prepared);
          return Mono.just(prepared);
        });
  }
}
