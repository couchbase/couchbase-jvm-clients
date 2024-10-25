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
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.topology.NodeIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Stability.Internal
public class ClassicCoreQueryResult extends CoreQueryResult {
  private final List<QueryChunkRow> rows;
  private final QueryChunkHeader header;
  private final QueryChunkTrailer trailer;
  private final NodeIdentifier lastDispatchedToNode;

  public ClassicCoreQueryResult(QueryChunkHeader header, List<QueryChunkRow> rows, QueryChunkTrailer trailer, NodeIdentifier lastDispatchedToNode) {
    this.rows = rows;
    this.header = header;
    this.trailer = trailer;
    this.lastDispatchedToNode = lastDispatchedToNode;
  }

  @Override
  public Stream<QueryChunkRow> rows() {
    return rows.stream();
  }

  public List<QueryChunkRow> collectRows() {
    return new ArrayList<>(rows);
  }

  public CoreQueryMetaData metaData() {
    return ClassicCoreQueryMetaData.from(header, trailer);
  }

  @Override
  public NodeIdentifier lastDispatchedTo() {
    return lastDispatchedToNode;
  }
}
