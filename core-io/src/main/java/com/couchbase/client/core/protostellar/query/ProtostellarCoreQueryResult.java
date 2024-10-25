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
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.util.List;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Volatile
public class ProtostellarCoreQueryResult extends CoreQueryResult {
  private final List<QueryResponse> responses;

  public ProtostellarCoreQueryResult(List<QueryResponse> responses) {
    this.responses = notNull(responses, "responses");
  }

  @Override
  public Stream<QueryChunkRow> rows() {
    return responses.stream()
      .flatMap(response -> response.getRowsList().stream())
      .map(row -> new QueryChunkRow(row.toByteArray()));
  }

  @Override
  public CoreQueryMetaData metaData() {
    return responses.stream()
      .filter(v -> v.hasMetaData())
      .map(v -> new ProtostellarCoreQueryMetaData(v.getMetaData()))
      .findFirst()
      .get();
  }

  @Override
  public NodeIdentifier lastDispatchedTo() {
    return null;
  }
}
