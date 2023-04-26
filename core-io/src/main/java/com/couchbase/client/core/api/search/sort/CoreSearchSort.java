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
package com.couchbase.client.core.api.search.sort;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.Sorting;
import reactor.util.annotation.Nullable;

@Stability.Internal
public abstract class CoreSearchSort {

  protected @Nullable Boolean descending;

  protected CoreSearchSort(@Nullable Boolean descending) {
    this.descending = descending;
  }

  protected abstract String identifier();

  public JsonNode toJsonNode() {
    ObjectNode node = Mapper.createObjectNode();
    injectParams(node);
    return node;
  }

  protected void injectParams(final ObjectNode queryJson) {
    queryJson.put("by", identifier());
    if (descending != null) {
      queryJson.put("desc", descending);
    }
  }

  public abstract Sorting asProtostellar();
}
