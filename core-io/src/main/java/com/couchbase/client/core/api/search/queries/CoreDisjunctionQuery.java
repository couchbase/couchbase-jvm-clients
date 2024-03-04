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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.DisjunctionQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

@Stability.Internal
public class CoreDisjunctionQuery extends CoreAbstractCompoundQuery {

  private final @Nullable Integer min;

  public CoreDisjunctionQuery(List<CoreSearchQuery> queries, @Nullable Integer min, @Nullable Double boost) {
    super(queries, boost);
    this.min = min;

    if (childQueries.isEmpty()) {
      throw InvalidArgumentException.fromMessage("Disjunction query has no child query");
    }
    if (min != null && childQueries.size() < min) {
      throw InvalidArgumentException.fromMessage("Disjunction query has fewer children than the configured minimum " + min);
    }
  }

  @Override
  protected void injectParams(ObjectNode input) {
    if (min != null) {
      input.put("min", min);
    }

    ArrayNode disjuncts = Mapper.createArrayNode();
    for (CoreSearchQuery childQuery : childQueries) {
      ObjectNode childJson = Mapper.createObjectNode();
      childQuery.injectParamsAndBoost(childJson);
      disjuncts.add(childJson);
    }
    input.set("disjuncts", disjuncts);
  }

  @Override
  public Query asProtostellar() {
    return Query.newBuilder().setDisjunctionQuery(asDisjunctionProtostellar()).build();
  }

  public DisjunctionQuery asDisjunctionProtostellar() {
    DisjunctionQuery.Builder query = DisjunctionQuery.newBuilder()
        .addAllQueries(childQueries.stream()
            .map(CoreSearchQuery::asProtostellar)
            .collect(Collectors.toList()));

    if (min != null) {
      query.setMinimum(min);
    }

    if (boost != null) {
      query.setBoost(boost.floatValue());
    }

    return query.build();
  }

}
