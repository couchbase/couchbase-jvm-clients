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

package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.node.NodeIdentifier;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The result of a N1QL query, including rows and associated metadata.
 *
 * @since 3.0.0
 */
@Stability.Internal
public abstract class CoreQueryResult {
  /**
   * Returns all rows.  As this is a Stream this is a once-through operation.
   */
  public abstract Stream<QueryChunkRow> rows();

  /**
   * Returns all rows in a buffered list.  As this is building on a Stream, this is a once-through operation.
   */
  public List<QueryChunkRow> collectRows() {
    return rows().collect(Collectors.toList());
  }

  /**
   * Returns the {@link CoreQueryMetaData} giving access to the additional metadata associated with this query.
   */
  public abstract CoreQueryMetaData metaData();

  /**
   * The last node the request was dispatched to.
   */
  public abstract @Nullable NodeIdentifier lastDispatchedTo();
}
