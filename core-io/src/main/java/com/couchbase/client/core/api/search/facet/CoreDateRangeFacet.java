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
package com.couchbase.client.core.api.search.facet;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

import java.util.List;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreDateRangeFacet extends CoreSearchFacet {
  private final List<CoreDateRange> dateRanges;

  public CoreDateRangeFacet(String field, @Nullable Integer limit, List<CoreDateRange> dateRanges) {
    super(field, limit);
    this.dateRanges = notNullOrEmpty(dateRanges, "Date Ranges");
  }

  public List<CoreDateRange> dateRanges() {
    return dateRanges;
  }

  @Override
  public void injectParams(ObjectNode queryJson) {
    super.injectParams(queryJson);

    ArrayNode dateRange = Mapper.createArrayNode();
    for (CoreDateRange dr : dateRanges) {
      ObjectNode drJson = Mapper.createObjectNode();
      drJson.put("name", dr.name());

      if (dr.start() != null) {
        drJson.put("start", dr.start());
      }
      if (dr.end() != null) {
        drJson.put("end", dr.end());
      }

      dateRange.add(drJson);
    }
    queryJson.set("date_ranges", dateRange);
  }
}
