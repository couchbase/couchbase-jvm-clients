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
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.BooleanQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreBooleanQuery extends CoreSearchQuery {

  private final @Nullable CoreConjunctionQuery must;
  private final @Nullable CoreDisjunctionQuery mustNot;
  private final @Nullable CoreDisjunctionQuery should;

  public CoreBooleanQuery(@Nullable CoreConjunctionQuery must,
                          @Nullable CoreDisjunctionQuery mustNot,
                          @Nullable CoreDisjunctionQuery should,
                          @Nullable Double boost) {
    super(boost);

    boolean mustIsEmpty = must == null || must.childQueries().isEmpty();
    boolean mustNotIsEmpty = mustNot == null || mustNot.childQueries().isEmpty();
    boolean shouldIsEmpty = should == null || should.childQueries().isEmpty();

    if (mustIsEmpty && mustNotIsEmpty && shouldIsEmpty) {
      throw InvalidArgumentException.fromMessage("Boolean query needs at least one of must, mustNot and should");
    }

    this.must = must;
    this.mustNot = mustNot;
    this.should = should;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    boolean mustIsEmpty = must == null || must.childQueries().isEmpty();
    boolean mustNotIsEmpty = mustNot == null || mustNot.childQueries().isEmpty();
    boolean shouldIsEmpty = should == null || should.childQueries().isEmpty();

    if (!mustIsEmpty) {
      ObjectNode jsonMust = Mapper.createObjectNode();
      must.injectParamsAndBoost(jsonMust);
      input.set("must", jsonMust);
    }

    if (!mustNotIsEmpty) {
      ObjectNode jsonMustNot = Mapper.createObjectNode();
      mustNot.injectParamsAndBoost(jsonMustNot);
      input.set("must_not", jsonMustNot);
    }

    if (!shouldIsEmpty) {
      ObjectNode jsonShould = Mapper.createObjectNode();
      should.injectParamsAndBoost(jsonShould);
      input.set("should", jsonShould);
    }
  }

  @Override
  public Query asProtostellar() {
    BooleanQuery.Builder query = BooleanQuery.newBuilder();

    if (must != null) {
      query.setMust(must.asConjunctionProtostellar());
    }

    if (mustNot != null) {
      query.setMustNot(mustNot.asDisjunctionProtostellar());
    }

    if (should != null) {
      query.setShould(should.asDisjunctionProtostellar());
    }

    if (boost != null) {
        query.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setBooleanQuery(query).build();
  }
}
