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

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of an analytics query, including rows and associated metadata.
 *
 * @since 3.0.0
 */
public class AnalyticsResult {

  /**
   * Stores the encoded rows from the analytics response.
   */
  private final List<AnalyticsChunkRow> rows;

  /**
   * The header holds associated metadata that came back before the rows streamed.
   */
  private final AnalyticsChunkHeader header;

  /**
   * The trailer holds associated metadata that came back after the rows streamed.
   */
  private final AnalyticsChunkTrailer trailer;

  /**
   * The default serializer to use.
   */
  private final JsonSerializer serializer;

  /**
   * Creates a new AnalyticsResult.
   *
   * @param header the analytics header.
   * @param rows the analytics rows.
   * @param trailer the analytics trailer.
   */
  AnalyticsResult(final AnalyticsChunkHeader header, final List<AnalyticsChunkRow> rows,
                  final AnalyticsChunkTrailer trailer, final JsonSerializer serializer) {
    this.rows = rows;
    this.header = header;
    this.trailer = trailer;
    this.serializer = serializer;
  }

  /**
   * Returns all rows, converted into instances of the target class.
   *
   * @param target the target class to deserialize into.
   * @param <T> the generic type to cast the rows into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of the generic target type.
   */
  public <T> List<T> rowsAs(final Class<T> target) {
    final List<T> converted = new ArrayList<>(rows.size());
    for (AnalyticsChunkRow row : rows) {
      converted.add(serializer.deserialize(target, row.data()));
    }
    return converted;
  }

  /**
   * Returns all rows, converted into instances of the target type.
   *
   * @param target the target type to deserialize into.
   * @param <T> the generic type to cast the rows into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of the generic target type.
   */
  public <T> List<T> rowsAs(final TypeRef<T> target) {
    final List<T> converted = new ArrayList<>(rows.size());
    for (AnalyticsChunkRow row : rows) {
      converted.add(serializer.deserialize(target, row.data()));
    }
    return converted;
  }

  /**
   * Returns all rows, converted into {@link JsonObject}s.
   *
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of JsonObjects.
   */
  public List<JsonObject> rowsAsObject() {
    return rowsAs(JsonObject.class);
  }

  /**
   * Returns the {@link AnalyticsMetaData} giving access to the additional metadata associated with this analytics
   * query.
   *
   * @return the analytics metadata.
   */
  public AnalyticsMetaData metaData() {
    return AnalyticsMetaData.from(header, trailer);
  }

  @Override
  public String toString() {
    return "AnalyticsResult{" +
      "rows=" + rows +
      ", header=" + header +
      ", trailer=" + trailer +
      ", serializer=" + serializer +
      '}';
  }
}
