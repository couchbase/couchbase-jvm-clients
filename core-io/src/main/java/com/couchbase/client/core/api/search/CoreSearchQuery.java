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
package com.couchbase.client.core.api.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.queries.CoreSearchQueryConverter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.topology.ClusterCapability;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

@Stability.Internal
public abstract class CoreSearchQuery {

  public @Nullable Double boost;

  protected CoreSearchQuery(@Nullable Double boost) {
    this.boost = boost;
  }

  public CoreSearchQuery boost(double boost) {
    this.boost = boost;
    return this;
  }


  protected abstract void injectParams(ObjectNode input);

  public abstract <T> T convert(CoreSearchQueryConverter<T> converter);

  /**
   * Returns the queries nested directly inside this one.
   */
  public List<CoreSearchQuery> childQueries() {
    return emptyList();
  }

  /**
   * Returns the cluster capabilities required by this query,
   * not counting the requirements of its child queries.
   */
  protected Set<ClusterCapability> ownRequiredCapabilities() {
    return emptySet();
  }

  /**
   * Adds to the given set any cluster capabilities required by this query
   * or its descendants.
   */
  public final void addRequiredCapabilities(Set<ClusterCapability> result) {
    result.addAll(ownRequiredCapabilities());
    childQueries().forEach(it -> it.addRequiredCapabilities(result));
  }

  public ObjectNode export() {
    ObjectNode out = Mapper.createObjectNode();
    injectParamsAndBoost(out);
    return out;
  }

  public void injectParamsAndBoost(ObjectNode json) {
    if (boost != null) {
      json.put("boost", boost.floatValue());
    }
    injectParams(json);
  }
}
