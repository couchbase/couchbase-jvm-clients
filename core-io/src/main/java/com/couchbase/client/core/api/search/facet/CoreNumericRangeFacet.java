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

@Stability.Internal
public class CoreNumericRangeFacet extends CoreSearchFacet {
  private final List<CoreNumericRange> ranges;

  public CoreNumericRangeFacet(String field, @Nullable Integer limit, List<CoreNumericRange> ranges) {
    super(field, limit);
    this.ranges = ranges;
  }

  @Override
  public void injectParams(ObjectNode queryJson) {
    super.injectParams(queryJson);

    ArrayNode numericRange = Mapper.createArrayNode();
    for (CoreNumericRange nr : ranges) {
      ObjectNode nrJson = Mapper.createObjectNode();
      nrJson.put("name", nr.name());

      if (nr.min() != null) {
        nrJson.put("min", nr.min());
      }
      if (nr.max() != null) {
        nrJson.put("max", nr.max());
      }

      numericRange.add(nrJson);
    }
    queryJson.set("numeric_ranges", numericRange);
  }
}
