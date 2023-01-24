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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.protostellar.query.v1.QueryResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class ProtostellarCoreReactiveQueryResult extends CoreReactiveQueryResult {

  private final Flux<QueryResponse> responses;

  public ProtostellarCoreReactiveQueryResult(Flux<QueryResponse> responses) {
    this.responses = notNull(responses, "responses");
  }

  public Flux<QueryChunkRow> rows() {
    return responses.flatMap(response -> Flux.fromIterable(response.getRowsList())
      .map(row -> new QueryChunkRow(row.toByteArray())));
  }

  public Mono<CoreQueryMetaData> metaData() {
    return responses.takeUntil(response -> response.hasMetaData())
      .single()
      .map(response -> new ProtostellarCoreQueryMetaData(response.getMetaData()));
  }

  @Override
  public NodeIdentifier lastDispatchedTo() {
    return null;
  }
}
