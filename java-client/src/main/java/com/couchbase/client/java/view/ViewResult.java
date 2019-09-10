/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.view;

import com.couchbase.client.core.msg.view.ViewChunkHeader;
import com.couchbase.client.core.msg.view.ViewChunkRow;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds a the result of a View request operation if successful.
 *
 * @since 3.0.0
 */
public class ViewResult {

  /**
   * Stores the encoded rows from the view response.
   */
  private final List<ViewChunkRow> rows;

  /**
   * The header holds associated metadata that came back before the rows streamed.
   */
  private final ViewChunkHeader header;

  /**
   * Creates a new ViewResult.
   *
   * @param header the view header.
   * @param rows the view rows.
   */
  ViewResult(final ViewChunkHeader header, final List<ViewChunkRow> rows) {
    this.rows = rows;
    this.header = header;
  }

  /**
   * Returns all view rows.
   */
  public List<ViewRow> rows() {
    final List<ViewRow> converted = new ArrayList<>(rows.size());
    for (ViewChunkRow row : rows) {
      converted.add(new ViewRow(row.data()));
    }
    return converted;
  }

  /**
   * Returns the {@link ViewMetaData} giving access to the additional metadata associated with this view query.
   */
  public ViewMetaData metaData() {
    return ViewMetaData.from(header);
  }

  @Override
  public String toString() {
    return "ViewResult{" +
      "rows=" + rows +
      ", header=" + header +
      '}';
  }

}