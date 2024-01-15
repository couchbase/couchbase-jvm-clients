/*
 * Copyright (c) 2024 Couchbase, Inc.
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
import com.couchbase.client.core.api.search.vector.CoreVectorSearch;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreSearchRequest {

  public final @Nullable CoreSearchQuery searchQuery;
  public final @Nullable CoreVectorSearch vectorSearch;

  public CoreSearchRequest(@Nullable CoreSearchQuery searchQuery,
                           @Nullable CoreVectorSearch vectorSearch) {
    if (searchQuery == null && vectorSearch == null) {
      throw new InvalidArgumentException("At least one of searchQuery and vectorSearch must be specified", null, null);
    }
    this.searchQuery = searchQuery;
    this.vectorSearch = vectorSearch;
  }

  public ObjectNode toJson() {
    ObjectNode topLevel = Mapper.createObjectNode();
    if (searchQuery != null) {
      ObjectNode query = Mapper.createObjectNode();
      searchQuery.injectParamsAndBoost(query);
      topLevel.put("query", query);
    }
    if (vectorSearch != null) {
      vectorSearch.injectTopLevel(topLevel);
    }
    return topLevel;
  }
}
