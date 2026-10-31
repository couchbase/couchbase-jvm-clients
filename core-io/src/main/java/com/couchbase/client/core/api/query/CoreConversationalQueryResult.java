/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreConversationalQueryResult {
  private final List<QueryChunkRow> rows;
  private final CoreConversationalQueryMetaData metaData;
  private final Optional<String> generatedStatement;

  public CoreConversationalQueryResult(List<QueryChunkRow> rows,
                                       CoreConversationalQueryMetaData metaData,
                                       Optional<String> generatedStatement) {
    this.rows = requireNonNull(rows);
    this.metaData = requireNonNull(metaData);
    this.generatedStatement = requireNonNull(generatedStatement);
  }

  public List<QueryChunkRow> rows() {
    return rows;
  }

  public CoreConversationalQueryMetaData metaData() {
    return metaData;
  }

  public Optional<String> generatedStatement() {
    return generatedStatement;
  }
}
