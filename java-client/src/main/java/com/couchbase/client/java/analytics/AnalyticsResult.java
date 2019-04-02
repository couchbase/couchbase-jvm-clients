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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.java.AsyncUtils.block;

/**
 * Holds the results (including metadata) of an analytics query.
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class AnalyticsResult {

  private final List<AnalyticsChunkRow> rows;
  private final AnalyticsChunkHeader header;
  private final AnalyticsChunkTrailer trailer;

  AnalyticsResult(AnalyticsChunkHeader header, List<AnalyticsChunkRow> rows, AnalyticsChunkTrailer trailer) {
    this.rows = rows;
    this.header = header;
    this.trailer = trailer;
  }


  public <T> Stream<T> rowsAs(final Class<T> target) {
    return rows.stream().map(row -> {
      try {
        return JacksonTransformers.MAPPER.readValue(row.data(), target);
      } catch (IOException e) {
        throw new DecodingFailedException("Decoding of Analytics Row failed!", e);
      }
    });
  }

  public <T> List<T> allRowsAs(final Class<T> target) {
    return rowsAs(target).collect(Collectors.toList());
  }

  /**
   * Returns all rows, converted into {@link JsonObject}s.
   * <p>
   * @throws DecodingFailedException if any row could not be successfully decoded
   */
  public Stream<JsonObject> rowsAsObject() {
    return rowsAs(JsonObject.class);
  }

  /**
   * Returns all rows, converted into {@link JsonObject}s.
   * <p>
   * @throws DecodingFailedException if any row could not be successfully decoded
   */
  public List<JsonObject> allRowsAsObject() {
    return allRowsAs(JsonObject.class);
  }

  public AnalyticsMeta meta() {
    return AnalyticsMeta.from(header, trailer);
  }

  @Override
  public String toString() {
    return "AnalyticsResult{" +
            "rows=" + rows +
            ", header=" + header +
            ", trailer=" + trailer +
            '}';
  }
}
