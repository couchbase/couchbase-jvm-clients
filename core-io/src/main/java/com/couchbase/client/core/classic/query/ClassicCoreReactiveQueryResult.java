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

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreReactiveQueryResult;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.node.NodeIdentifier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class ClassicCoreReactiveQueryResult extends CoreReactiveQueryResult {

  private final QueryResponse response;
  private final @Nullable NodeIdentifier lastDispatchedTo;

  @Stability.Internal
  public ClassicCoreReactiveQueryResult(QueryResponse response, @Nullable NodeIdentifier lastDispatchedTo) {
    this.response = notNull(response, "response");
    this.lastDispatchedTo = lastDispatchedTo;
  }

  public Flux<QueryChunkRow> rows() {
    return response.rows();
  }

  public Mono<CoreQueryMetaData> metaData() {
    return response.trailer().map(t -> ClassicCoreQueryMetaData.from(response.header(), t));
  }

  @Override
  public NodeIdentifier lastDispatchedTo() {
    return lastDispatchedTo;
  }

  public QueryResponse internal() {
    return response;
  }
}
