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

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.view.ViewResponse;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ViewResult {

  private final ViewResponse response;

  public ViewResult(final ViewResponse response) {
    this.response = response;
  }

  public Stream<ViewRow> rows() {
    return response.rows().map(r -> new ViewRow(r.data())).toStream();
  }

  public List<ViewRow> allRows() {
    return rows().collect(Collectors.toList());
  }

  public ViewMeta meta() {
    return new ViewMeta(
      response.header().debug().map(bytes -> Mapper.decodeInto(bytes, JsonObject.class)),
      response.header().totalRows()
    );
  }

}