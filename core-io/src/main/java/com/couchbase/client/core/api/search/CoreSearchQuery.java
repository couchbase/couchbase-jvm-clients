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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.Query;

@Stability.Internal
public abstract class CoreSearchQuery {

  protected Double boost;

  protected CoreSearchQuery(Double boost) {
    this.boost = boost;
  }

  public CoreSearchQuery boost(double boost) {
    this.boost = boost;
    return this;
  }


  protected abstract void injectParams(ObjectNode input);

  public abstract Query asProtostellar();

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
