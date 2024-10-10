/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.columnar.client.java.internal.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Holds associated metadata returned by the server.
 */
@ThreadSafe
public final class QueryMetadata {

  private final AnalyticsChunkHeader header;
  private final AnalyticsChunkTrailer trailer;

  QueryMetadata(final AnalyticsChunkHeader header, final AnalyticsChunkTrailer trailer) {
    this.header = header;
    this.trailer = trailer;
  }

  /**
   * Get the request identifier of the query request
   *
   * @return request identifier
   */
  public String requestId() {
    return header.requestId();
  }

  /**
   * Get the associated metrics for the response.
   *
   * @return the metrics for the analytics response.
   */
  public QueryMetrics metrics() {
    return new QueryMetrics(trailer.metrics());
  }

  /**
   * Returns warnings if present.
   *
   * @return warnings, if present.
   */
  public List<QueryWarning> warnings() {
    return this.trailer.warnings().map(warnings ->
      ErrorCodeAndMessage.fromJsonArray(warnings).stream().map(QueryWarning::new).collect(Collectors.toList())
    ).orElse(Collections.emptyList());
  }

  @Override
  public String toString() {
    return "QueryMetadata{" +
      "header=" + header +
      ", trailer=" + trailer +
      '}';
  }
}
