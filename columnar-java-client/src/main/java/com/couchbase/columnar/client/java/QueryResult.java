/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.columnar.client.java.internal.ThreadSafe;

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;

@ThreadSafe(caveat = "Unless you modify the byte array returned by Row.bytes()")
public final class QueryResult {
  private final List<Row> rows;
  private final QueryMetadata metadata;

  QueryResult(
    AnalyticsChunkHeader header,
    List<Row> rows,
    AnalyticsChunkTrailer trailer
  ) {
    this.rows = listCopyOf(rows);
    this.metadata = new QueryMetadata(header, trailer);
  }

  public List<Row> rows() {
    return rows;
  }

  public QueryMetadata metadata() {
    return metadata;
  }

}
