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
package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.NumericRangeQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreNumericRangeQuery extends CoreSearchQuery {

  private final @Nullable Double min;
  private final @Nullable Double max;
  private final @Nullable Boolean inclusiveMin;
  private final @Nullable Boolean inclusiveMax;
  private final @Nullable String field;

  public CoreNumericRangeQuery(@Nullable Double min,
                               @Nullable Double max,
                               @Nullable Boolean inclusiveMin,
                               @Nullable Boolean inclusiveMax,
                               @Nullable String field,
                               @Nullable Double boost) {
    super(boost);

    if (min == null && max == null) {
      throw new NullPointerException("NumericRangeQuery needs at least one of min or max");
    }

    this.min = min;
    this.max = max;
    this.inclusiveMin = inclusiveMin;
    this.inclusiveMax = inclusiveMax;
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    if (min != null) {
      input.put("min", min);
      if (inclusiveMin != null) {
        input.put("inclusive_min", inclusiveMin);
      }
    }
    if (max != null) {
      input.put("max", max);
      if (inclusiveMax != null) {
        input.put("inclusive_max", inclusiveMax);
      }
    }
    if (field != null) {
      input.put("field", field);
    }
  }

  @Override
  public Query asProtostellar() {
    NumericRangeQuery.Builder builder = NumericRangeQuery.newBuilder();

    if (min != null) {
      builder.setMin(min.floatValue());
      if (inclusiveMin != null) {
        builder.setInclusiveMin(inclusiveMin);
      }
    }
    if (max != null) {
      builder.setMax(max.floatValue());
      if (inclusiveMax != null) {
        builder.setInclusiveMax(inclusiveMax);
      }
    }
    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setNumericRangeQuery(builder).build();

  }
}
