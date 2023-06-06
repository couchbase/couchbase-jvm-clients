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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Stability.Internal
public class ProtostellarCoreReactiveQueryResult extends CoreReactiveQueryResult {

  private final Flux<QueryChunkRow> rows;
  private final  Mono<CoreQueryMetaData> metaData;

  public ProtostellarCoreReactiveQueryResult(Flux<QueryChunkRow> rows, Mono<CoreQueryMetaData> metaData) {
    this.rows = rows;
    this.metaData = metaData;
  }

  public Flux<QueryChunkRow> rows() {
    return rows;
  }

  public Mono<CoreQueryMetaData> metaData() {
    return metaData;
  }

  @Override
  public NodeIdentifier lastDispatchedTo() {
    return null;
  }
}
